import fanstatic
import js.jquery

library = fanstatic.Library('flot', 'resources')

def less_than_ie8_renderer(url):
    return ('<!--[if lte IE 8]><script language="javascript" '
        'type="text/javascript" src="%s"></script><![endif]-->') % url

excanvas = fanstatic.Resource(library, 'excanvas.js',
    minified='excanvas.min.js', renderer=less_than_ie8_renderer)

flot = fanstatic.Resource(library, 'jquery.flot.js',
    minified='jquery.flot.min.js', depends=[js.jquery.jquery, excanvas])

selection = fanstatic.Resource(library, 'jquery.flot.selection.js',
    minified='jquery.flot.selection.min.js', depends=[flot])
