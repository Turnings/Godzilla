import os
import re

import magic
from django.db import models
from django.contrib.auth.models import User
from django.conf import settings
from django.db.models import F
from django.utils import timezone

from .libs import make_abspath


class File(models.Model):
    """ 通用的文件类 """
    # 文件名字
    name = models.CharField(max_length=256)
    # 拥有者
    owner = models.ForeignKey(User)
    # 所属的父目录
    parent = models.ForeignKey('File', null=True)

    # 后面的文件对象，通过property的方法根据文件类型，
    # 指向文件对象或目录对象
    object_pk = models.IntegerField(null=True)
    is_regular = models.BooleanField(default=True)  # directory/file

    @property
    def object(self):
        if self.is_regular:
            return RegularFile.objects.get(pk=self.object_pk)
        else:
            return DirectoryFile.objects.get(pk=self.object_pk)

    def link(self, fo):
        """建立File与RegularFile/DirectoryFile之间的链接"""
        self.object_pk = fo.pk
        if isinstance(fo, RegularFile):
            fo.links += 1
            fo.save()
        self.save()

    def unlink(self):
        # 如果是最后一个指向文件的名字，则可以删除文件系统上的文件
        assert self.is_regular, "only support regular file deletion"
        fo = self.object
        if fo.links == 1:
            abspath = make_abspath(fo.path)
            try:
                os.remove(abspath)
            except FileNotFoundError:
                ...
            fo.delete()
        else:
            fo.links = F('links') - 1
            fo.save()
        self.delete()

    def add(self, other):
        """往目录中添加子目录或常规文件"""
        fo = self.object
        assert isinstance(fo, DirectoryFile), "only valid for directories"

        if other.is_regular:
            name = 'files'
        else:
            name = 'subdirs'
        text = ':%s' % other.pk
        value = getattr(fo, name)
        if text not in value:
            setattr(fo, name, value + text)
            fo.size = len(fo.subdirs) + len(fo.files)
            fo.save()
            other.parent = self
            other.save()

    def remove(self, other):
        fo = self.object
        assert isinstance(fo, DirectoryFile), "only valid for directories"

        if other.is_regular:
            name = 'files'
        else:
            name = 'subdirs'
        text = ':%s' % other.pk
        value = getattr(fo, name)
        if text in value:
            setattr(fo, name, value.replace(text, ''))
            fo.size = len(fo.subdirs) + len(fo.files)
            fo.save()

    def mimetype(self):
        if not self.is_regular:
            return "dir"

        maps = [('image', r'^image/.*'),
                ('audio', r'^audio/.*'),
                ('video', r'^video/.*'),
                ('text', r'^text/.*'),
                ('pdf', r'^application/pdf$'),
                ('gzip', r'^application/gzip$'),
                ('bzip2', r'^application/x-bzip2$'),
                ('zip', r'^application/zip$'),
                ('tar', r'^application/x-tar$')]
        type_text = self.raw_mimetype()
        m = [name for name, pat in maps if re.match(pat, type_text)]
        if m:
            return m[0]
        else:
            return 'octet'

    def raw_mimetype(self):
        mime = magic.Magic(mime=True)
        path = os.path.join(settings.MEDIA_ROOT, self.object.path)
        return mime.from_file(path)

    def is_viewable(self):
        return self.mimetype() in ['pdf', 'text', 'image', 'audio', 'video']

    def shares(self):
        """查找文件本身及所有父目录所有的共享，返回迭代器，由近到远"""

        def _shares(node):
            return [x for x in node.share_set.all() if not x.is_expired()]

        # 查找文件本身的共享
        for s in _shares(self):
            yield s, 'self'

        # 查找所有的父目录的共享信息
        node = self
        while node.parent:
            parent = node.parent
            node = parent
            for s in _shares(node):
                yield s, 'parent'

    def shared_status(self):
        """根据文件的共享状态返回字符串"""
        status = []
        for share, text in self.shares():
            if share.code is None:
                status.append('[anonymous]/%s' % text)
            else:
                status.append('%s/%s' % (share.code, text))
        res = ', '.join(status)
        return res or 'private'

    def shared_to_all(self):
        # 判断文件是不是匿名共享了
        for s, _ in self.shares():
            if s.code is None:
                return True
        return False

    def shared_with_code(self):
        for s, _ in self.shares():
            if s.code is not None:
                return True
        return False

    def abspath(self):
        """文件的绝对路径"""
        node = self
        names = [node.name]
        while node.parent:
            node = node.parent
            names.append(node.name)
        return '/' + '/'.join(names[::-1])


class DirectoryFile(models.Model):
    # 下一级节点，子目录/文件的表示法：':123:34567:15379'
    subdirs = models.CharField(max_length=204800, default='')
    files = models.CharField(max_length=204800, default='')
    # 创建时间
    time = models.DateTimeField(auto_now_add=True)
    # 目录的大小: subdirs和files的总长度
    size = models.IntegerField(default=0)


class RegularFile(models.Model):
    # 文件尺寸
    size = models.IntegerField()
    # 已经接收的数据量（用于断点续传）
    received = models.IntegerField()
    # 开始上传时间
    time = models.DateTimeField(auto_now_add=True)
    # 文件的校验和 (sha1)
    digest = models.CharField(max_length=40)
    # 文件在服务器上的文件系统中的相对路径
    path = models.CharField(max_length=4096)
    # 文件上传是否完成
    finished = models.BooleanField(default=False)
    # 文件的链接数（类似文件系统的硬链接）
    links = models.IntegerField(default=0)


class Share(models.Model):
    target = models.ForeignKey('File')
    # 提取码，当为None时，表示是匿名下载
    code = models.CharField(max_length=8, null=True)
    # 该共享的失效时间
    expire = models.DateTimeField(null=True)

    def is_expired(self):
        return self.expire is not None and self.expire <= timezone.now()


class Quota(models.Model):
    user = models.OneToOneField(User)
    total = models.IntegerField()
    used = models.IntegerField(default=0)

    def is_sufficient(self, delta):
        return self.total >= self.used + delta
