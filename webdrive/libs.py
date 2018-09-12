import os
import random

from django.conf import settings


def make_abspath(path):
    return os.path.join(settings.MEDIA_ROOT, path)


def gen_code(length=6):
    """Generate a random string"""
    nums = list(range(97, 123)) + list(range(65, 91)) + list(range(48, 58))
    chars = [chr(x) for x in nums]
    res = [random.choice(chars) for _ in range(length)]
    return ''.join(res)
