from pyramid.view import view_config
from pyramid.response import Response

@view_config(route_name='vocabs')
def vocab(request):
    return Response('')

@view_config(route_name='vocab')
def show(request):
    return Response('')

@view_config(route_name='add_vocab', request_method='POST')
def add_vocab(request):
    return Response('')