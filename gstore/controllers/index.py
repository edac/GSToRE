# -*- coding: utf-8 -*-
"""Main Controller"""
import logging
from pylons import request, response, config
from pylons.controllers.util import abort
from pylons import tmpl_context as c
from pylons.templating import render_mako as render

from gstore.lib.base import BaseController

log = logging.getLogger(__name__)

__all__ = ['IndexController']

class IndexController(BaseController):
    def index(self):
        c.rest = render('api.txt') 
        return render('index.html')
