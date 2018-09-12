"""
这是用来访问文件共享服务的命令行客户端，可以独立运行，不依赖文件共享服务程序的代码。

功能：

1. 获取登录用户的文件列表 (list dir)
2. 获取文件的详情，适用于登录用户/匿名访问/分享码访问 (detail)
3. 下载单个文件，适用于登录用户/匿名访问/分享码访问 (download)
4. 下载文件时支持断点续传
5. 上传单个文件，适用于登录用户 (upload)
6. 上传文件时支持断点续传
7. 上传时不重复上传服务器上已有的文件，通过计算校验和来实现 (秒传)
8. 上传时，如果已经存在可用的文件校验和，则不重复计算校验和
9. 支持下载多个文件或者目录
10. 支持上传多个文件或者目录
11. 支持session和登入登出

"""

import os
from os.path import dirname, basename, abspath, exists, isdir, isfile
import sys

import json
import requests

basedir = dirname(abspath(__file__))
sys.path.insert(0, basedir)
from thinap import ArgParser


def help():
    text = """available commands: login logout ls mkdir rmdir cp fetch

路径表示法：

    1. 远程路径的开始标记是一个冒号，冒号之后是路径，
        路径以斜杠开始就是绝对路径，否则就是相对于家目录的路径。
    2. 本地路径的表示法与Linux的文件系统表示法相同。

login/logout 命令用于登入和登出，登入成功后服务器的信息会被记录下来，供后续命令使用。

ls 命令用于列出远程文件/目录的信息

    -l 参数显示详情，不加-l 则只显示名字
    -d 参数只会影响目录的显示，有此参数则列出目录本身，无则列出目录内容

mkdir 命令用于创建远程目录

    -p 参数用于创建不存在的父目录，目标存在时也不出错
    -v 参数用于显示过程

cp 命令用于上传下载，远程路径写在前面是下载，写在后面是上传

    -o 参数使得不上传服务器上已有的文件 （秒传）
    -r 参数用于上传下载目录
    -v 参数用于显示过程

fetch 命令用于下载分享的文件或目录。

    -O 参数用于指定下载的文件（非目录）的本地存放路径
    -P 参数用于指定路径的前缀（不影响-O 参数）

    如果需要提供分享码，则分享码写在url中，形式如下（假设分享码是abcd）：

    http://abcd@hostname/path/to/target


范例：

1. login/logout

    login -u alice -p password      登入
    logout                          登出

2. ls 命令

    ls                  列出家目录下所有的子目录和文件的名字
    ls data             列出家目录下的data中所有子目录和文件的名字
    ls -ld videos       列出家目录下的videos 目录本身的详细信息
    ls -l videos        列出家目录下的videos 目录中所有子目录和文件的详情

3. 在远程家目录下创建目录 multimedia/anv，如果multimedia 不存在，就一并创建

    mkdir -p multimedia/anv

4. 上传本地文件 genesis.mp3 到远程 multimedia/anv/ 目录下面

    cp genesis.mp3 :multimedia/anv/

5. 上传多个本地文件到远程

    cp genesis.mp3 goodday.mp4 :multimedia/anv/

6. 上传文件和/或目录到远程的家目录中

    cp -r multimedia calculus.pdf data.tar :
    cp -r multimedia :

7. 下载远程文件，存放到本地的当前目录中（留意命令后面的点）

    cp :calculus.pdf .

8. 下载远程文件和目录，存放到本地的fetched目录中

    cp -r :multimedia :calculus.pdf fetched

9. fetch 命令

    fetch http://host/download/13/                  下载到当前目录
    fetch -O /tmp/a.mp3 http://host/download/13/    指定保存的路径
    fetch -P downloaded http://host/download/13/    指定文件存储的目录
"""
    print(text, end='')


def save_session(data):
    path = '/tmp/.client_of_share_session'
    with open(path, 'w') as f:
        f.write(json.dumps(data))


def load_session():
    path = '/tmp/.client_of_share_session'
    if not exists(path):
        return {}
    with open(path) as f:
        return json.loads(f.read())


def login(args, api):
    """登入，并保存cookie"""
    request = {'username': {'flag': '-u', 'arg': 1},
               'password': {'flag': '-p', 'arg': 1}}
    p = ArgParser()
    params = p.parse_args(args, request)
    mapping = params[0]
    username = mapping.get('username')
    password = mapping.get('password')
    assert username and password, 'user name and password are required'

    data = {'username': username, 'password': password}
    res, r = send_request(api, data, send_cookies=False)
    if not res:
        return False

    if res['status']:
        sid = r.cookies.get('sessionid')
        if sid:
            save_session({'sessionid': sid})
            return True
    else:
        print(res['errors'])
        return False


def logout(args, api):
    """登出，并清除cookie"""
    path = '/tmp/.client_of_share_session'
    r = requests.get(api)
    save_session({})
    return True


def send_request(api, data=None, files=None, send_cookies=True):
    if send_cookies:
        cookies = load_session()
    else:
        cookies = {}
    r = requests.post(api, data=data, files=files, cookies=cookies)
    if r.ok:
        return r.json(), r
    else:
        print('request failed (code %s)' % r.status_code)
        return None, None


def ls(args, api):
    request = {'long': {'flag': '-l'},
               'directory': {'flag': '-d'}}
    p = ArgParser()
    params = p.parse_args(args, request)
    mapping = params[0]
    long = mapping.get('long', False)
    directory = mapping.get('directory', False)
    names = params[1] or []

    data = dict(long=long, directory=directory, names=names)
    res, r = send_request(api, data)
    if not res:
        return False

    # 输出文件的信息
    if long:
        files = []
        for block in res['output']:
            files.extend(list(block.values())[0])
        format_output(files)
    else:
        for block in res['output']:
            for key, files in block.items():
                if not files:
                    continue
                for file in files:
                    print(file)
    # 输出错误信息
    if not res['status']:
        for e in res['errors']:
            print('error:', e)
        return False
    else:
        return True


def format_output(files):
    # 字段：regular, owner, size, time, name
    sizes = [len(str(f['size'])) for f in files]
    size_len = max(sizes)
    for f in files:
        type = '-' if f['regular'] else 'd'
        fmt = '%%s %%s %%%ds %%s %%s' % size_len
        line = fmt % (type, f['owner'], f['size'], f['time'], f['name'])
        print(line)


def mkdir(args, api):
    request = {'parents': {'flag': '-p'},
               'verbose': {'flag': '-v'}}
    p = ArgParser()
    params = p.parse_args(args, request)
    mapping = params[0]
    parents = mapping.get('parents', False)
    verbose = mapping.get('verbose', False)
    names = params[1] or []

    data = dict(parents=parents, verbose=verbose, names=names)
    res, r = send_request(api, data)
    if not res:
        return False

    # -v, 输出详细信息
    if verbose:
        for name in res['output']:
            print('created directory: %s' % name)

    # 输出错误信息
    if not res['status']:
        for e in res['errors']:
            print('error:', e)
        return False
    else:
        return True


def rmdir(args, api):
    request = {'parents': {'flag': '-p'},
               'verbose': {'flag': '-v'}}
    p = ArgParser()
    params = p.parse_args(args, request)
    mapping = params[0]
    parents = mapping.get('parents', False)
    verbose = mapping.get('verbose', False)
    names = params[1] or []

    data = dict(parents=parents, verbose=verbose, names=names)
    res, r = send_request(api, data)
    if not res:
        return False

    # -v, 输出详细信息
    if verbose:
        for name in res['output']:
            print('removing directory: %s' % name)

    # 输出错误信息
    if not res['status']:
        for e in res['errors']:
            print('error:', e)
        return False
    else:
        return True


def cp(args, api):
    """
    上传下载

    操作目录，断点续传，秒传。

    -o 参数使得不上传服务器上已有的文件 （秒传）
    -r 参数用于上传下载目录
    -v 参数用于显示过程

    """
    request = {'one': {'flag': '-o'},
               'recursive': {'flag': '-r'},
               'verbose': {'flag': '-v'}}
    p = ArgParser()
    params = p.parse_args(args, request)
    mapping = params[0]
    one = mapping.get('one', False)
    recursive = mapping.get('recursive', False)
    verbose = mapping.get('verbose', False)
    names = params[1] or []
    src, dst, is_upload = process_names(names)

    res = True
    for src_path in src:
        status, errmsg = upload(src_path, dst, one, recursive, verbose)
        if not status:
            print(errmsg)
            res = False
    return res


def upload(src, dst, one, recursive, verbose):
    mkdir_api = apis.get('mkdir')
    if isdir(src):  # upload a whole direcroty
        if not recursive:
            return False, "omitting directory: %s" % src
        dirs = [(src, dst)]
        # 当第一次检测远程目录是否存在时，这个目录是本次上传
        # 操作的顶级目录，后续的子目录没有必要再做此检测。
        assume_not_exists = False

        while dirs:
            # 首先处理目录本身：在远程创建目录
            dir, dst_dir = dirs.pop(0)
            if assume_not_exists or not rexists(dst_dir):
                new_dir = dst_dir
                assume_not_exists = True
            else:
                new_dir = os.path.join(dst_dir, dir)
            args = [new_dir, '-p']  # 创建不存在的父目录
            if verbose:
                args.append('-v')
            assert mkdir(args, mkdir_api)

            # 然后处理目录下面所有的子目录和文件
            for sub in os.listdir(dir):
                path = os.path.join(dir, sub)
                if isdir(path):
                    dirs.append((path, os.path.join(dst_dir, sub)))
                elif isfile(path):
                    dst = os.path.join(dst_dir, sub)
                    status, errmsg = upload_one_file(path, dst, one, verbose)
    else:
        args = [dirname(dst), '-p']  # 为文件预备远程目录
        assert mkdir(args, mkdir_api)
        status, errmsg = upload_one_file(src, dst, one, verbose)

    return status, errmsg


def upload_one_file(src, dst, one, verbose):
    if verbose:
        def put(*args, **kargs):
            print(*args, **kargs)
    else:
        def put(*args, **kargs):
            ...

    # 秒传模式
    if one:
        # 计算源文件的sha1校验和，或者从已有的记录文件中加载校验和
        put('calculating/loading digest')
        sha1 = digest_of_path(src)
        status = one_mode_upload(src, dst, sha1)
        if status:
            return True, None

    #
    # 秒传条件不成立，或者当前不是秒传模式，开始使用常规方式上传
    #

    # 询问服务器，文件是否已经上传了一部分
    offset = remote_file_offset(src, dst)
    if offset == -1:    # 文件已经上传完整，无需再传
        True, None

    # 开始上传文件的数据
    sfile = open(src, 'rb')
    sfile.seek(offset)
    api = apis.get('upload')
    put('uploading %s -> %s' % (src, dst))
    res, r = send_request(api, data={'offset': offset},
                          files={basename(src): sfile})
    assert res
    return res['status'], res['errors']


def process_names(names):
    """
    1. 把names分成两份，一份是源路径，一份是目标路径。
    2. 确保以下几点：
        1. 源文件是本地文件/目录时，必须存在
        2. 目标是本地常规文件时，不存在，但其父目录必须存在
        3. 目标是本地目录文件时，必须存在

    names是来自类似以下命令的文件名：

    cp genesis.mp3 :multimedia/anv/
    cp genesis.mp3 goodday.mp4 :multimedia/anv/
    cp -r multimedia calculus.pdf data.tar :
    cp -r multimedia :
    cp :calculus.pdf .
    cp -r :multimedia :calculus.pdf fetched
    """

    names_len = len(names)
    assert names_len >= 2, "expect at least two file names"
    if names_len == 2:
        src, dst = names
        if (src.startswith(':') and dst.startswith(':')
                or not src.startswith(':') and not dst.startswith(':')):
            assert False, "wrong source or destination address"
        src = [src]
    elif names_len > 2:
        src = names[:-1]
        dst = names[-1]
        chars = [x[0] for x in src]
        if (dst.startswith(':') and chars.count(':') != 0
                or not dst.startswith(':') and chars.count(':') != len(chars)):
            assert False, "wrong source or destination address"

    is_upload = dst.startswith(':')
    if is_upload:
        # 源文件是本地文件/目录时，必须存在
        for name in src:
            assert exists(name), "file not exists: %s" % name
    else:
        if exists(dst): # 存在，则必须是目录文件
            assert isdir(dst), "destination exists but is not a directory"
        else:   # 不存在，推定为常规文件，其父目录必须存在
            p = dirname(dst)
            assert isdir(p), "%s not exists or not a directory" % p

    dst = dst[1:]   # strip the leading colon
    return src, dst, is_upload


def fetch(args, api):
    ...


def rexists(path):
    """ 如果远程路径存在就返回True，否则返回False """
    api = apis.get('exists')
    data = dict(name=path)
    res, r = send_request(api, data)
    assert res
    return res['status']


def remote_file_offset(src, dst):
    """ 查询服务器上某文件已经上传完成的大小 """
    api = apis.get('offset')
    data = dict(src=src, dst=dst)
    res, r = send_request(api, data)
    assert res
    return res['offset']


commands = {
    'login': login,
    'logout': logout,
    'ls': ls,
    'mkdir': mkdir,
    'rmdir': rmdir,
    'cp': cp,
    'fetch': fetch,
}

apis = {
    'login': 'http://127.0.0.1:8000/webdrive/api/login/',
    'logout': 'http://127.0.0.1:8000/webdrive/api/logout/',
    'ls': 'http://127.0.0.1:8000/webdrive/api/ls/',
    'mkdir': 'http://127.0.0.1:8000/webdrive/api/mkdir/',
    'rmdir': 'http://127.0.0.1:8000/webdrive/api/rmdir/',
    'cp': 'http://127.0.0.1:8000/webdrive/api/cp/',
    'fetch': 'http://127.0.0.1:8000/webdrive/api/fetch/',
    'exists': 'http://127.0.0.1:8000/webdrive/api/exists/',
    'offset': 'http://127.0.0.1:8000/webdrive/api/offset/',
    'upload': 'http://127.0.0.1:8000/webdrive/api/upload/',
}

if __name__ == '__main__':
    if '--help' in sys.argv:
        help()
        exit(0)
    elif len(sys.argv) < 2:
        fmt = 'usage: %(name)s <command> [options] [arguments]'
        fmt += '\nfor more info, run %(name)s --help'
        print(fmt % {'name': basename(sys.argv[0])})
        exit(1)

    cmd = sys.argv[1]
    args = sys.argv[2:]

    command = commands.get(cmd)

    # 无效的命令
    if not command:
        print('invalid command %s' % cmd)
        exit(1)

    # 执行命令
    api = apis.get(cmd)
    try:
        status = command(args, api)
    except Exception as e:
        print(e)
        exit(1)
    exit(0 if status else 1)
