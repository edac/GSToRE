from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1278184987.9179699
_template_filename='/var/gstore/trunk/gstore/templates/index.html'
_template_uri='index.html'
_template_cache=cache.Cache(__name__, _modified_time)
_source_encoding='utf-8'
from webhelpers.html import escape
_exports = []


def render_body(context,**pageargs):
    context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        c = context.get('c', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 1
        __M_writer(u'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html \n     PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"\n    "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">\n<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">\n    <head>\n        <title>GSTORE: Geographic Storage and Retrieval Engine!</title>\n        <link rel=\'stylesheet\' href=\'/style.css\'></link>\n    </head>\n    <body>\n        <div id="page">\n           <div id="header"> \n            <h1>GSTORE</h1>\n                <h2>Geographic Storage and Retrieval Engine</h2>\n            </div>\n            <div id="toplinks">\n                <h2>Help</h2>\n                <ul>\n                    <li>\n                        <a href="http://pylonshq.com/docs/en/1.0/">Official documentation</a>\n                    </li>\n                    <li>\n                        <a href="http://wiki.pylonshq.com/display/pylonsfaq/Home">FAQ</a>\n                    </li>\n                    <li>\n                        <a href="http://wiki.pylonshq.com/dashboard.action">Wiki</a>\n                    </li>\n                    <li>\n                        <a href="http://wiki.pylonshq.com/display/pylonscommunity/Home#Home-JointheMailingLists">Mailing list</a>\n                    </li>\n                    <li>\n                        <a href="http://wiki.pylonshq.com/display/pylonscommunity/Home#Home-IRC">IRC</a>\n                    </li>\n                    <li>\n                        <a href="http://pylonshq.com/project/pylonshq/roadmap">Bug tracker</a>\n                    </li>\n                </ul>\n            </div>\n            <div id="main">\n                <h2>REST API Reference</h2>\n                <div id=\'rest\'>')
        # SOURCE LINE 41
        __M_writer(escape(c.rest))
        __M_writer(u' </div>\n            </div>\n            <div id="footer">\n                <a href="http://edac.unm.edu" style="color: #ccc; text-decoration:none;"><img src=\'/images/edac-logo.gif\' align=\'left\' border=\'0\' width=\'60px\' alt=\'Earth Data Analysis Center\' title=\'Earth Data Analysis Center\'/></a>\n                <a href=\'http://www.unm.edu\'><img src=\'/images/unm-logo.gif\' align=\'right\' border=\'0\' width=\'100px\' alt=\'University of New Mexico\' title=\'University of New Mexico\'/></a>\n\n All content Copyright \xa9 2010\n  by the <a href="http://edac.unm.edu">Earth Data Analysis Center</a><br/> \n      <span>Send all comments to <a href="mailto:webmaster@edac.unm.edu">webmaster</a>.\n      </span> \n            </div>\n        </div>\n    </body>\n</html>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


