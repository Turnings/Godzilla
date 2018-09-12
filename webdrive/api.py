import os

from django.contrib import auth
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404
from django.http import Http404
from django.conf import settings

from .models import File, DirectoryFile


@csrf_exempt
@require_POST
def login(request):
    username = request.POST.get('username')
    password = request.POST.get('password')
    user = auth.authenticate(username=username, password=password)
    if user is not None and user.is_active:
        auth.login(request, user)
        data = {'status': True}
    else:
        data = {'status': False, 'errors': 'login failed'}
    return JsonResponse(data)


def logout(request):
    auth.logout(request)
    data = {'status': True}
    return JsonResponse(data)


def inform_login(request):
    """给客户端发送适当的登录提示信息"""
    data = {'status': False, 'output': [{}], 'errors': ['login required']}
    return JsonResponse(data)


@login_required(login_url=settings.API_LOGIN_URL)
@csrf_exempt
def ls(request):
    user = request.user
    long = request.POST.get('long', '') == 'True'
    directory = request.POST.get('directory', '') == 'True'
    names = request.POST.getlist('names', [])
    home = get_object_or_404(File, owner=user, name=user.username,
                             is_regular=False, parent=None)

    # 第一步，取出所有的名字对应的文件或目录
    if not names:
        # list the home directory
        files = [home]
        errors = []
    else:
        files, errors = paths_to_files(names, home)

    # 第二步，有必要时列出目录的内容
    if directory:
        files = [{'flat': files}]
    else:
        dirs = [x for x in files if not x.is_regular]
        regs = [x for x in files if x.is_regular]
        files = []
        for dir in dirs:
            subs = File.objects.filter(parent=dir)
            label = getattr(dir, 'requested_path', dir.name)
            files.append({label: subs})
        files.append({'regular': regs})

    output = render_ls_output(files, long)
    res = {'status': not bool(errors), 'output': output, 'errors': errors}
    return JsonResponse(res)


def paths_to_files(paths, home):

    """ 把客户端提交过来的原始路径对应的文件提取出来 """

    files = []
    errors = []
    for path in paths:
        abspath = transform_path(path, home)
        if abspath is None:
            errors.append('no permission on %s' % path)
            continue
        file = resolve_abspath(abspath, home)
        if file:
            # 保存原始的请求路径，用于客户端显示结果
            file.requested_path = path
            files.append(file)
        else:
            errors.append('file not found: %s' % path)
    return files, errors


def collect_path_objects(path, home):
    # names 是相对于家目录的路径中所有的文件名字
    names = path.split('/')[2:]
    parent = home
    objs = [home]
    try:
        for name in names:
            obj = get_object_or_404(File, name=name, parent=parent)
            parent = obj
            objs.append(parent)
    except Http404:
        ...
    return objs


def resolve_abspath(path, home):
    # names 是相对于家目录的路径中所有的文件名字
    names = path.split('/')[2:]
    parent = home
    try:
        for name in names:
            obj = get_object_or_404(File, name=name, parent=parent)
            parent = obj
        return obj
    except Http404:
        return None


def transform_path(path, home):
    """
    把相对路径转成绝对路径，当绝对路径不在家目录下时报错，
    处理路径中可能存在的连续斜杠，及尾部的斜杠。
    """
    home_path = '/%s/' % home.name

    # 相对路径
    if not path.startswith('/'):
        path = home_path + path
    # 不在家目录下的绝对路径
    elif path != home_path[:-1] and not path.startswith(home_path):
        return None
    # 处理连续的斜杠
    path = os.path.normpath(path)
    return path


def render_ls_output(files, long):
    # 抽取文件的详细信息，或者仅仅抽取文件的名字
    res = []
    if not long:
        for file_info in files:
            for key, file_objs in file_info.items():
                res.append({key: [getattr(f, 'requested_path', f.name)
                                    for f in file_objs]})
    else:
        for file_info in files:
            for key, file_objs in file_info.items():
                records = []
                for f in file_objs:
                    # 抽取字段：type, owner, size, time, name
                    record = {'regular': f.is_regular,
                              'owner': f.owner.username,
                              'size': f.object.size,
                              'time': f.object.time.strftime('%F %T'),
                              'name': getattr(f, 'requested_path', f.name)}
                    records.append(record)
                res.append({key: records})
    return res


@login_required(login_url=settings.API_LOGIN_URL)
@csrf_exempt
def mkdir(request):
    user = request.user
    opt_parents = request.POST.get('parents', '') == 'True'
    names = request.POST.getlist('names', [])
    home = get_object_or_404(File, owner=user, name=user.username,
                             is_regular=False, parent=None)

    created = []
    errors = []
    for name in names:
        abspath = transform_path(name, home)
        if abspath is None:
            errors.append('no permission on %s' % name)
            continue
        objs = collect_path_objects(abspath, home)
        path_elements = abspath.split('/')[1:]
        exists_num = len(objs)
        request_num = len(path_elements)
        if request_num == exists_num:  # exists
            if not opt_parents:
                errors.append('cannot create %s: file exists' % name)

        # 目标不存在，可以创建
        else:
            if request_num - exists_num == 1:
                # 父目录全部存在，可以直接创建
                create_directory(path_elements[-1], user, objs[-1])
                created.append(name)
            else:
                # 缺少部分父目录
                if opt_parents:     # -d选项, 同时创建不存在的父目录
                    parent = objs[-1]
                    for dir_name in path_elements[exists_num:]:
                        dir = create_directory(dir_name, user, parent)
                        created.append(dir_name)
                        parent = dir
                else:               # 出错
                    errmsg = 'cannot create %s: parent not exists' % name
                    errors.append(errmsg)

    res = {'status': not bool(errors),
           'output': created,
           'errors': errors}
    return JsonResponse(res)


@login_required(login_url=settings.API_LOGIN_URL)
@csrf_exempt
def rmdir(request):
    user = request.user
    opt_parents = request.POST.get('parents', '') == 'True'
    names = request.POST.getlist('names', [])
    home = get_object_or_404(File, owner=user, name=user.username,
                             is_regular=False, parent=None)

    removed = []
    errors = []
    for name in names:
        abspath = transform_path(name, home)
        objs = collect_path_objects(abspath, home)
        for file in objs[-1:0:-1]:    # revert and exclude the home directory
            if file.is_regular:
                errmsg = 'failed to remove: %s: not a directory' % name
                errors.append(errmsg)
            elif file.object.size == 0:
                delete_directory(file)
                removed.append(file.abspath())
            else:
                errmsg = 'failed to remove: %s: directory not empty' % name
                errors.append(errmsg)

    res = {'status': not bool(errors),
           'output': removed,
           'errors': errors}
    return JsonResponse(res)


def create_directory(name, owner, parent=None):
    fo = DirectoryFile.objects.create()
    dir = File.objects.create(name=name, owner=owner, is_regular=False)
    dir.link(fo)
    if parent:
        parent.add(dir)
    return dir


def delete_directory(dir):
    dir.parent.remove(dir)
    dir.object.delete()
    dir.delete()


@login_required(login_url=settings.API_LOGIN_URL)
@csrf_exempt
def exists(request):
    user = request.user
    name = request.POST.get('name')
    home = get_object_or_404(File, owner=user, name=user.username,
                             is_regular=False, parent=None)

    files, errors = paths_to_files([name], home)
    res = {'status': bool(files), 'errors': errors}
    return JsonResponse(res)


@login_required(login_url=settings.API_LOGIN_URL)
@csrf_exempt
def offset(request):
    user = request.user
    src = request.POST.get('src')
    dst = request.POST.get('dst')
    home = get_object_or_404(File, owner=user, name=user.username,
                             is_regular=False, parent=None)

    files, errors = paths_to_files([dst], home)
    res = {'status': bool(files), 'errors': errors}
    return JsonResponse(res)
