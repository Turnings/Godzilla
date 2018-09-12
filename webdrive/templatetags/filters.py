from django import template

register = template.Library()

@register.filter(name="countsub")
def do_countsub(val):
    return val.count(':')
