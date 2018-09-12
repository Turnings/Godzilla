from django.conf.urls import url

from . import views
from . import api

urlpatterns = [
    url(r'^(?:^page/(?P<page>[0-9]+)/)?$', views.index, name='index'),
    url(r'^list/(?P<pk>[0-9a-z]+)/(?:page/(?P<page>[0-9]+)/)?$',
        views.list_dir, name='list_dir'),
    url(r'^login/$', views.login, name='login'),
    url(r'^logout/$', views.logout, name='logout'),
    url(r'^signup/$', views.signup, name='signup'),
    url(r'^detail/(?P<pk>[0-9]+)/$', views.detail, name='detail'),
    url(r'^view/(?P<pk>[0-9]+)/$', views.view, name='view'),
    url(r'^download/(?P<pk>[0-9]+)/$', views.download, name='download'),
    url(r'^edit/(?P<pk>[0-9]+)/$', views.edit, name='edit'),
    url(r'^delete/(?P<pk>[0-9]+)/$', views.delete, name='delete'),
    url(r'^share/list/(?:page/(?P<page>[0-9]+)/)?$', views.list_shares, name='list_shares'),
    url(r'^share/create/(?P<pk>[0-9]+)/$', views.create_share, name='create_share'),
    url(r'^share/edit/(?P<pk>[0-9]+)/$', views.edit_share, name='edit_share'),
    url(r'^share/delete/(?P<pk>[0-9]+)/$', views.delete_share, name='delete_share'),
    url(r'^submit_code/(?P<pk>[0-9]+)/$', views.submit_code, name='submit_code'),
    url(r'^captcha/', views.captcha, name='captcha'),
    url(r'^search/', views.search, name='search'),
    url(r'^upload/', views.upload, name='upload'),

    url(r'^api/login/', api.login, name='api_login'),
    url(r'^api/inform_login/', api.inform_login, name='api_inform_login'),
    url(r'^api/ls/', api.ls, name='api_ls'),
    url(r'^api/mkdir/', api.mkdir, name='api_mkdir'),
    url(r'^api/rmdir/', api.rmdir, name='api_rmdir'),
    url(r'^api/exists/', api.exists, name='api_exists'),
]
