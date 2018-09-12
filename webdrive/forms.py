from django import forms


class LoginForm(forms.Form):
    username = forms.CharField()
    password = forms.CharField(widget=forms.PasswordInput())
    captcha = forms.CharField(max_length=12)


class RenameForm(forms.Form):
    name = forms.CharField()


class ShareForm(forms.Form):
    code = forms.CharField(max_length=8, min_length=6, required=False)
    expire = forms.DateTimeField(required=False, disabled=True)
    anonymous = forms.BooleanField(required=False)
    never_expire = forms.BooleanField(required=False, initial=True)


class UploadForm(forms.Form):
    files = forms.FileField(widget=forms.ClearableFileInput(attrs={'multiple': True}))
