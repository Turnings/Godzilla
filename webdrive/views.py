import os
import re
import hashlib
from io import BytesIO
from tempfile import NamedTemporaryFile

from django.shortcuts import render, get_object_or_404
from django import urls
from django.urls import reverse
from django.http import (HttpResponseRedirect, StreamingHttpResponse,
                         HttpResponseBadRequest, HttpResponse, Http404)
from django.contrib import auth
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from .forms import LoginForm, RenameForm, ShareForm, UploadForm
from .models import DirectoryFile, RegularFile, File, Share, Quota
from .libs import make_abspath, gen_code
from .views_libs import (create_directory, get_session_id,
                         get_session_data, set_session_data,
                         share_approved, permission_ok, make_image, gentext,
                         make_path, get_items)


@login_required
def index(request, page=1):
    """
    用户主页，显示用户资源的相关链接：文件，共享。
    """
    user = request.user
    home = get_object_or_404(File, name=user.username, owner=user,
                             is_regular=False, parent=None)
    return list_dir(request, dir=home, page=page)


@login_required
def list_dir(request, pk=None, dir=None, page=1):
    """查看目录下的文件"""
    assert pk is not None or dir is not None, "pk or dir is required"
    if dir is None:
        user = request.user
        dir = get_object_or_404(File, pk=pk, owner=user)
    files, parents = get_items(dir)

    # 分页
    paginator = Paginator(files, settings.PAGE_SIZE)
    try:
        files = paginator.page(page)
    except PageNotAnInteger:
        # 如果page不是整数，就选择第一页
        files = paginator.page(1)
    except EmptyPage:
        # 如果page超出范围，比如说9999, 就选择最后一页
        files = paginator.page(paginator.num_pages)

    context = {'files': files, 'parents': parents, 'title': 'File list'}
    return render(request, 'webdrive/list_dir.html', context=context)

@require_POST
def submit_code(request, pk):
    """ 提交提取码 """
    file = get_object_or_404(File, pk=pk)
    if request.method == 'POST':
        code = request.POST['code']
        for share, _ in file.shares():
            if share.code == code:
                shares = get_session_data(request, 'shares') or []
                shares.append(share.pk)
                set_session_data(request, 'shares', shares)
                url = reverse('webdrive:detail', args=(pk,))
                return HttpResponseRedirect(url)
        return HttpResponse('invalid code')


def detail(request, pk):
    """查看文件详情"""
    file = get_object_or_404(File, pk=pk)
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    context = {'file': file}
    if request.user.is_authenticated():
        # 登录用户，显示所有信息
        template_file = "webdrive/detail.html"
        context['title'] = 'File detail'
    elif file.shared_to_all():
        # 匿名共享文件，显示基本信息及下载链接
        template_file = "webdrive/detail_share_to_all.html"
        context['title'] = 'Shared file info'
    elif file.shared_with_code():
        # 分享码共享文件，显示基本信息，
        # 如果已经提交了有效的分享码，就显示链接，
        # 否则，就显示提交分享码的表单
        template_file = "webdrive/detail_share_with_code.html"
        context['title'] = 'Shared file info'
        context['approved'] = share_approved(request, file)
    else:
        url = reverse('webdrive:login') + '?next=' + request.META['PATH_INFO']
        return HttpResponseRedirect(url)
    return render(request, template_file, context=context)


def view(request, pk):
    """查看文件内容"""
    file = get_object_or_404(File, pk=pk)

    # 如果是常规文件的话，就必须是已经上传完毕的文件
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    # 客户端必须有相应的权限：已经登录（是文件的所有者），提交了提取码，匿名共享
    if not permission_ok(request, file):
        url = reverse('webdrive:login') + '?next=' + request.META['PATH_INFO']
        return HttpResponseRedirect(url)

    # 文件必须是可以view的那种类型
    if file.is_viewable():
        abspath = make_abspath(file.object.path)
        buf = open(abspath, 'rb')
        response = StreamingHttpResponse(buf)
        response['Content-Type'] = file.raw_mimetype()
        return response
    else:
        context = {'file': file, 'title': 'View file content'}
        return render(request, 'webdrive/view.html', context=context)


def download(request, pk):
    """下载文件"""
    file = get_object_or_404(File, pk=pk)
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    if not permission_ok(request, file):
        url = reverse('webdrive:login') + '?next=' + request.META['PATH_INFO']
        return HttpResponseRedirect(url)

    if not file.is_regular:
        return HttpResponseBadRequest("Only a regular file can be downloaded.")

    abspath = make_abspath(file.object.path)
    buf = open(abspath, 'rb')
    response = StreamingHttpResponse(buf)
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Length'] = str(file.object.size)
    response['Content-Disposition'] = 'attachment;filename="%s"' % file.name
    return response


@login_required
def list_shares(request, page=1):
    """查看所有的共享"""
    user = request.user
    now = timezone.now()
    shares = Share.objects.filter(target__owner=user)
    shares = [x for x in shares if not x.is_expired()]
    shares = sorted(shares, key=(lambda x: x.target.is_regular))

    # 分页
    paginator = Paginator(shares, settings.PAGE_SIZE)
    try:
        shares = paginator.page(page)
    except PageNotAnInteger:
        # 如果page不是整数，就选择第一页
        shares = paginator.page(1)
    except EmptyPage:
        # 如果page超出范围，比如说9999, 就选择最后一页
        shares = paginator.page(paginator.num_pages)

    context = {'shares': shares, 'title': 'Share list'}
    return render(request, 'webdrive/list_shares.html', context=context)


@login_required
def create_share(request, pk):
    """共享文件"""
    file = get_object_or_404(File, pk=pk)
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    if request.method == 'POST':
        form = ShareForm(request.POST)
        form.errors.clear()
        try:
            if 'anonymous' in request.POST:
                code = None
            else:
                code = form['code'].field.clean(request.POST['code'])
        except ValidationError as e:
            form.add_error('code', e)
        try:
            if 'never_expire' in request.POST:
                expire = None
            else:
                expire = form['expire'].field.clean(request.POST['expire'])
        except ValidationError as e:
            form.add_error('expire', e)

        if not form.errors:
            Share.objects.create(target=file, code=code, expire=expire)
            url = reverse('webdrive:detail', args=(pk,))
            return HttpResponseRedirect(url)

        form['code'].field.disabled = 'anonymous' in request.POST
        form['expire'].field.disabled = 'never_expire' in request.POST
    else:
        form = ShareForm(initial={'code': gen_code()})

    notice = ('note: if you share a directory, all files under '
              'the directory tree will be shared.')
    context={'file_name': file.name, 'form': form,
             'notice': notice, 'title': 'Create share'}
    return render(request, 'webdrive/create_share.html', context=context)


@login_required
def edit_share(request, pk):
    """修改共享"""
    share = get_object_or_404(Share, pk=pk)
    if request.method == 'POST':
        form = ShareForm(request.POST)
        form.errors.clear()
        try:
            if 'anonymous' in request.POST:
                code = None
            else:
                code = form['code'].field.clean(request.POST['code'])
        except ValidationError as e:
            form.add_error('code', e)
        try:
            if 'never_expire' in request.POST:
                expire = None
            else:
                expire = form['expire'].field.clean(request.POST['expire'])
        except ValidationError as e:
            form.add_error('expire', e)

        if not form.errors:
            share.code = code
            share.expire = expire
            share.save()
            next_url = request.POST['next']
            return HttpResponseRedirect(next_url)

        form['code'].field.disabled = 'anonymous' in request.POST
        form['expire'].field.disabled = 'never_expire' in request.POST
    else:
        init_data = {'code': share.code, 'expire': share.expire,
                     'anonymous': share.code is None,
                     'never_expire': share.expire is None}
        form = ShareForm(initial=init_data)
        form['code'].field.disabled = init_data['anonymous']
        form['expire'].field.disabled = init_data['never_expire']

    next_url = request.META['HTTP_REFERER']
    context={'file_name': share.target.name, 'form': form,
             'next': next_url, 'title': 'Edit share'}
    return render(request, 'webdrive/create_share.html', context=context)


@login_required
def delete_share(request, pk):
    """删除共享"""
    share = get_object_or_404(Share, pk=pk)
    if request.method == 'POST':
        if request.POST.get('submit', '') == 'Delete':
            share.delete()
            return HttpResponseRedirect(request.POST['next'])
    next_url = request.META['HTTP_REFERER']
    context={'file_name': share.target.name, 'next': next_url,
             'title': 'Delete share'}
    return render(request, 'webdrive/delete_share.html', context=context)


@login_required
def edit(request, pk):
    """修改文件"""
    file = get_object_or_404(File, pk=pk)
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    if request.method == 'POST':
        form = RenameForm(request.POST)
        if form.is_valid():
            name = form.cleaned_data['name']
            file.name = name
            file.save()
            return HttpResponseRedirect(reverse('webdrive:detail', args=(pk,)))
    else:
        form = RenameForm({'name': file.name})
    context = {'form': form, 'title': 'Edit file'}
    return render(request, 'webdrive/edit.html', context=context)


@login_required
def delete(request, pk):
    """删除文件"""
    file = get_object_or_404(File, pk=pk)
    if file.is_regular and not file.object.finished:
        raise Http404('No File matches the given query')

    if not file.is_regular:
        return HttpResponseBadRequest("Only a regular file can be deleted.")

    if request.method == 'POST':
        if request.POST.get('submit', '') == 'Delete':
            # 首先更新磁盘配额
            quota = request.user.quota
            quota.used -= file.object.size
            quota.save()
            # 然后删除文件
            file.unlink()
            next_url = request.POST['next']
            return HttpResponseRedirect(next_url)
    next_url = request.META['HTTP_REFERER']
    context={'file_name': file.name, 'next': next_url, 'title': 'Delete file'}
    return render(request, 'webdrive/delete.html', context=context)


def login(request):
    next_url = request.GET.get('next', reverse('webdrive:index'))
    if request.user.is_authenticated():
        return HttpResponseRedirect(next_url)

    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            # 先对比验证码，再校验用户名字和密码
            posted_captcha = form.cleaned_data['captcha']
            saved_captcha = get_session_data(request, 'captcha')
            if (saved_captcha is not None
                    and posted_captcha.lower() == saved_captcha):
                username = form.cleaned_data['username']
                password = form.cleaned_data['password']
                user = auth.authenticate(username=username, password=password)
                if user is not None and user.is_active:
                    auth.login(request, user)
                    return HttpResponseRedirect(next_url)
            else:
                form.add_error('captcha','验证码不匹配')
    else:
        form = LoginForm()
    context = {'form': form, 'title': 'Login', 'login_page': True}
    return render(request, 'webdrive/login.html', context=context)


def logout(request):
    auth.logout(request)
    return HttpResponseRedirect(reverse('webdrive:index'))


def signup(request):
    if request.method == 'POST':
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            home = create_directory(name=user.username, owner=user)
            Quota.objects.create(user=user, total=settings.DEFAULT_QUOTA)
            username = user.username
            raw_password = form.cleaned_data.get('password1')
            user = auth.authenticate(username=username, password=raw_password)
            auth.login(request, user)
            return HttpResponseRedirect(reverse('webdrive:index'))
    else:
        form = UserCreationForm()
    context = {'form': form, 'title': 'Sign up'}
    return render(request, 'webdrive/signup.html', context=context)


def captcha(request):
    """ 生成验证码图片 """
    text = gentext(4)
    # 把验证码内容存到session中
    set_session_data(request, 'captcha', text.lower())
    im = make_image(text)
    imgout = BytesIO()
    im.save(imgout, format='png')
    img_bytes = imgout.getvalue()
    return HttpResponse(img_bytes, content_type='image/png')


@login_required
def search(request):
    user = request.user
    pattern = request.GET.get('pattern')
    try:
        files = list(File.objects.filter(owner=user, name__regex=pattern))
        files = [f for f in files if getattr(f.object, 'finished', True)]
    except Exception:
        files = []

    # 分页
    page = request.GET.get('page')
    paginator = Paginator(files, settings.PAGE_SIZE)
    try:
        files = paginator.page(page)
    except PageNotAnInteger:
        # 如果page不是整数，就选择第一页
        files = paginator.page(1)
    except EmptyPage:
        # 如果page超出范围，比如说9999, 就选择最后一页
        files = paginator.page(paginator.num_pages)

    qs = request.META['QUERY_STRING']
    qs = re.sub('&page=[0-9]+', '', qs)
    qs = re.sub('page=[0-9]+&', '', qs)
    context = {'files': files, 'title': 'Search result',
               'pattern': pattern, 'qs': qs}
    return render(request, 'webdrive/search.html', context=context)


@login_required
def upload(request):
    if request.method == 'POST':
        form = UploadForm(request.POST, request.FILES)
        next_url = request.POST['next']
        if form.is_valid():
            user = request.user
            pk = urls.resolve(next_url).kwargs.get('pk', None)
            if pk:
                dir = get_object_or_404(File, pk=pk, owner=user)
            else:
                # if no pk available, it is under the HOME directory
                dir = get_object_or_404(File, name=user.username, owner=user)
            files = request.FILES.getlist('files')
            for file in files:
                quota = user.quota
                size = file._size
                if quota.is_sufficient(size):
                    quota.used += size
                    quota.save()
                    handle_uploaded_file(file, user, dir)
                else:
                    # 空间不足
                    context = {'required': size,
                               'available': quota.total - quota.used}
                    return render(request, 'webdrive/quota_error.html',
                                  context, status=402)
            return HttpResponseRedirect(next_url)
    else:
        form = UploadForm()
        try:
            # HTTP_REFERER 表示是通过点击(哪个)页面上的链接进来的
            next_url = request.META['HTTP_REFERER']
        except KeyError:
            return HttpResponseBadRequest("HTTP_REFERER info is required.")
        next_url = '/' + '/'.join(next_url.split('/')[3:])
    context = {'form': form, 'title': 'Upload files', 'next': next_url}
    return render(request, 'webdrive/upload.html', context=context)


def handle_uploaded_file(ufile, owner, dir):
    """ 接受上传的文件数据 """
    time = timezone.now()
    store_dir = make_path(time)
    store_dir = os.path.join(settings.MEDIA_ROOT, store_dir)
    os.makedirs(store_dir, mode=0o755, exist_ok=True)
    tmpfile = NamedTemporaryFile(dir=store_dir, delete=False)

    # 接收数据
    fo = RegularFile.objects.create(size=0, received=0, digest='',
                                     path=tmpfile.name, finished=False)
    read = 0
    hash = hashlib.sha1()
    for chunk in ufile.chunks(chunk_size=512):
        tmpfile.write(chunk)
        hash.update(chunk)
        read += len(chunk)
        fo.received = read
        fo.save()
    fo.size = read
    fo.digest = hash.hexdigest()
    fo.path = make_path(time, fo.digest)
    fo.finished = True
    abspath = os.path.join(settings.MEDIA_ROOT, fo.path)
    os.rename(tmpfile.name, abspath)
    fo.save()

    # 链接，加入目录
    file = File.objects.create(name=ufile.name, owner=owner)
    file.link(fo)
    dir.add(file)
