import os
import string
import random

from PIL import Image, ImageDraw, ImageFont
from django.conf import settings

from .models import DirectoryFile, File


def create_directory(name, owner):
    # 创建目录文件对象: 提供数据（数据块） fo: file object
    fo = DirectoryFile.objects.create()

    # 创建通用文件对象: 提供基本信息（元数据）
    dir = File.objects.create(name=name, owner=owner, is_regular=False)

    # 把通用文件对象和目录文件对象链在一起
    dir.link(fo)

    return dir


def get_session_id(request):
    name = settings.SESSION_COOKIE_NAME
    sid = request.COOKIES.get(name)
    if sid is None:
        request.session.create()
        sid = request.session.session_key
    return sid


def get_session_data(request, key, sid=None):
    if sid is None:
        sid = get_session_id(request)
    return request.session.get(sid, {}).get(key)


def set_session_data(request, key, data, sid=None):
    if sid is None:
        sid = get_session_id(request)
    sdata = request.session.get(sid, {})
    sdata[key] = data
    request.session[sid] = sdata


def share_approved(request, file):
    approved_shares = get_session_data(request, 'shares') or []
    for share, _ in file.shares():
        if share.pk in approved_shares:
            approved = True
            break
    else:
        approved = False
    return approved


def permission_ok(request, file):
    return (request.user.is_authenticated() or file.shared_to_all()
            or (file.shared_with_code() and share_approved(request, file)))


def make_image(char):
    """ 生成验证码图片 """
    im_size = (70, 40)
    font_size = 28
    bg = (0, 0, 0)
    offset = (1, 1)
    im = Image.new('RGB', size=im_size, color=bg)
    font_path_relative = 'webdrive/fonts/ubuntu.ttf'
    font_path = os.path.join(settings.STATIC_ROOT, font_path_relative)
    if not os.path.exists(font_path):
        font_path = os.path.join('webdrive/static', font_path_relative)
    font = ImageFont.truetype(font_path, font_size)
    draw = ImageDraw.ImageDraw(im)
    draw.text(offset, char, fill='yellow', font=font)
    im = im.transform(im_size, Image.AFFINE, (1, -0.3, 0, -0.1, 1, 0), Image.BILINEAR)
    return im


def gentext(n=4):
    """ 生成n个字母的随机字符串 """
    chars = string.ascii_letters
    return ''.join([random.choice(chars) for i in range(n)])


def make_path(time, digest=''):
    return os.path.join(time.strftime('%Y%m%d'), digest)


def get_items(dir):
    # 列出目錄下的內容，就是子目錄和文件，同時返回所有父目錄
    files = records_from_ids(dir.object.subdirs + dir.object.files)
    parents = [dir]
    while dir.parent:
        parents.append(dir.parent)
        dir = dir.parent
    parents = parents[::-1]
    return files, parents


def records_from_ids(ids):
    # ids format: :id1:id2:id3
    # 每一个id的前面都有一个冒号
    if not ids:
        return []
    ids = [int(id) for id in ids.strip(':').split(':')]
    files = File.objects.filter(pk__in=ids).order_by('is_regular', 'name')
    return [f for f in files if getattr(f.object, 'finished', True)]
