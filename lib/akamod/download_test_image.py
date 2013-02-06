
from amara.thirdparty import json, httplib2
from amara.lib.iri import join
from StringIO import StringIO
from akara import module_config
import pprint
import sys
import re
import hashlib
import os
import os.path
import urllib
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

"""
This is a module used only for internal testing the module download_preview.

It just serves the images based on the request param.

This file contains images:
    image_png - png version, base64 format
    image_jpg - jpg version, base64 format

The image is taken from Clemson, the original URL is:
http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/cfb&CISOPTR=1042

"""

image_png = """iVBORw0KGgoAAAANSUhEUgAAAFcAAAB4CAIAAAAIUPnYAAAACXBIWXMAAAsTAAALEwEAmpwYAAAA
B3RJTUUH3QEVEAAUPi9cQQAAIABJREFUeNpcu2mMZVlyHhYRZ7nLu2/Pvfal972nZyGHPRwOqeGI
omVZImlDomzBPyRYgPXHgmXAEEzIPwzDBgwbNmwLsGyaErcxrWWmh9vMsGft6WWml+ru6qqu6q6s
rNzz7Xc9W/jHy6oe+fzITGRevHNunC8ivoj4Eu/tfXBysH/n5i1w1rlggs96/Wmed+K14/x4dWvF
VRYq6rV7o/mJ7CiqG+JuUU/jbBFCQL+WRt3KHEFknaU06VrXtFpxYypgBaw05ZNx2WkPlBaL/ERK
UrLV1MHEPpUUMRsHUdZpXCF40Umc8gQUSZE2TYVsNRBahSGmCK21RCSRlIp2d+6dO3duMplQjBCa
bpbMxrN8VveHAwcNU2MWQir0rmZ2SdKqStPrr5ZlIyXKKKqqMktE09QV6Ke/+FfR2NG7b7356ve+
0yxmwJaEGKyuVM6gJa8DS2APKaRNXnXX+6UvY1NJsVpVRZwVdV2bsu09D1cor9KV4fpsWkgpA5so
ioq8CQF1ivnCSJEQBecrqVCKhDCC2FfjcTuOfMDc2JX1Qaqbs6tZO6utl2VhEVFLfvlb37589mFb
gaNyMBjkeZ7GyWKxiGR07969TqdjgpeCz2ys3r2z3YozoaJFOen0WqnqvvX2m5ubq1kaj8fj1fUz
J8eTS5euOGeE0s5Y9NZ7PzP4wi//hlRClYt5s5hoqPttrSNO49wFD2yzQe9kPu23+/Vs7IXttWMn
CRbTVLcK0QScQwKqRWW9IJp1+8NIVlG7WVvbqOuauXCZ73a7i2ZBm4mzDWCQir03ztWSsrQny3az
2o2Nh9G8WTvjRahSuShmeSsdyEgLiWUx/9lPP2JK393oN6iUwrObwzIviE27pS5deLgsy0DUbXcO
d3effvzM/u7e+YsreSmsd2iLX3zxqbxcAMDZrXPOhSvnLi4WUxlLwACaJ4fHArCaGrSFdMZX+UJL
PjfILpzppxEI6QFgMOzWIBq3ooQYtluH+wc661QOkrO2FZ2JVTvPd4wxzrU3Lzw9mV7vn336aPvW
2taab9x8VvUHK97X8/kkiDhrDerKMfi0JUJwIRB7Xbiie+FsFMmmsk+0z00Wk246DNWk6a+2uuvT
4xylaHdkOz1TF4acrNB1u935fL6xtfnIoDe5s900zea5rcrVO3d2Xvj8Z31TXHpoq7HmTLxVFI5s
sVgsLjz0aJK2rDXz2UwAb53vNbVlJgJRLyYSNU5tkiTin/yT37r1wXvzk7v9jNZ6KotCrEOvEwnE
WKV/8mfffuyhR4IpV1eHH93efvf6bRbFO29/3G51UJhbH93++kvf2jiz0erL7/7Zx6++/la3s/rq
q9e+/rUfrK09dHQ8f/m7P3z8ocdb0QrbSKLWIlJRR4ne7Kh46707k+PFh9fvzOe+t3Lpf/gf/89I
ds6fewRjqEr3O7/7R2+9+36r1/36n3z9yWee+fjuvf/5n/5+d7h+5sJDb7x57b/9b/77n//FL3vS
33/1jTKYk3F+5tzla9fff/UnPyps/ecvv7J+5iEO5Ut//s033nn/6pPP3Ds6fu3Hr1vwg5W+TBJS
Wqtkb/cgoDqcVleff5EAiAMqpSSBAAYMzhh2fr5YjKfld15+47XXr01mhXFusqhe+sZ3WA3/7Nuv
fPDhHRAUEN669sE3X341N/Hb7753+87tOBMXrp49Gp/cuPWhCfXzn31CReS4EpqjVNS2tM18dHzv
X33t/9nevf3Us489/tTDx7PD0fx473gn7rXm5aKqLekUo1arM+yvbH7u536htboBWhuIf/Dae0fT
+tbO8dxwUO3vv/728bxJesNLjz57a3v03/1P//S5z33+0mOPX3702f/lf/9dUOn5q4/vHM7+r9/9
l93V83Fn5YnnPmUAGgaP0rFwHqwLTDqQIu/BM0gdSx2rOImTNpDyLFuD/s3tu5978Rd+5w//6MyV
y6M837hw+XheV7kanZRKRYJwdWX4/PMvvPXmjY9vjXqDyPhx2jZXH954/oVH//TP/9XuwY3LDw2m
1ejm9o33b7/74d2b79++drI4nlbj7776nU89dxFwkrWqz37m8tkrveOTWwcHH3TXoixOJWM5L268
f+Olr33twrnzk4O9frfzlb/85bfefuPG9fdbWbS1uZa1o1amv/nnL/2z/+O379zeqytOVKZ13M26
589e3P74yNa2FbX/+r/37//kx9e+991XV1c266L2xkIImhC90wToXaJJUCAhJRA6z1VjGuMYBGJM
lMyK+d7R8fr5ize3919/+83u6vDjvXtxp/fOT65rkV69dNk3Jbjmsy98eufO/ofX75xZfQbMwBRp
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"""

image_jpg = """/9j/4AAQSkZJRgABAQABLAEsAAD/2wBDAAYEBQYFBAYGBQYHBwYIChAKCgkJChQODwwQFxQYGBcU
FhYaHSUfGhsjHBYWICwgIyYnKSopGR8tMC0oMCUoKSj/2wBDAQcHBwoIChMKChMoGhYaKCgoKCgo
KCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCj/wAARCAB4AFcDAREA
AhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQA
AAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3
ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWm
p6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEA
AwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSEx
BhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElK
U1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3
uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD0e+sB
f+KtXMssqrHOUVUbAA6dK8fF150pWid9ClGcdShPLplumtENeM+lRiZ080fvEIPI/wCBK4+q+4rK
OJrO2u4OlTVyBrzTRa2d4y3q2k121o8omGEIYoH6cqWAH40e3rXtfWxXs4WvYt2lxbTxaey2d4qX
zywxFrnpKjEBGwvGQrHPsav6xVV9dvIj2cOxoXH/AAjkFveS/wBoyStaxPNJHFNuYBRluKX1mtp/
kHs4Eoht7WbTfNtpwbyZIl/0ncF3I75Ix22Y9OetQ8RUkmr/AIB7OKtoSO0F3rl5pixSA2irIZFl
K7gUz2XHUgYz3z2rNVJpJ3LVk9jFh1O0u41C2l/5iW8sssaTlmWSN1RowAMsfmzkduxzWrlNfaBT
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"""

@simple_service('GET', 'http://purl.org/la/dp/download_test_image',
    'download_test_image', 'application/json')

def download_preview(extension):
    """
    Special test only module.

    Arguments:
        extension String - extension of a file to return

    Returns:
        an image in png/jpg format with proper MIME type in header.

        Extension
        "png"   -   returns image in png format
        "jpg"   -   returns image in jpg format
        "bad"   -   returns no image, "content-type" = "very/bad"
        "500"   -   returns code 500

    """

    ext_to_mime = {
            'jpg': {'mime': 'image/jpeg', 'file': image_jpg},
            'png': {'mime': 'image/png',  'file': image_png},
            }

    if extension in ext_to_mime:
        response.add_header('content-type', ext_to_mime[extension]['mime'])
        return ext_to_mime[extension]['file'].decode('base64')
    elif extension == 'bad':
        response.add_header('content-type', "very/bad")
        return ""
    elif extension == "500":
        msg = "500 - as you asked"
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg
    else:
        msg = "Bad extension, got [%s]." % extension
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg
