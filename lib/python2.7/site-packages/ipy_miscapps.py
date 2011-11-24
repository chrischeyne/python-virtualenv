""" Completers for miscellaneous command line apps

Examples::

    %options_remember ./configure --help
      (after this, you can do ./configure --<tab>)
"""
import IPython.ipapi
ip = IPython.ipapi.get()
import os,  re

def surfraw_completer(self,event):
    """ Completer for 'surfraw'

    example::
      sr go<tab>  => sr google

    """
    compl = [l.split(None,1)[0] for l in os.popen('sr -elvi')]
    return compl


ip.set_hook('complete_command', surfraw_completer, str_key = 'sr')

_syscmds = None

def shellcmd_completer(self,  event):
    """ Check if this is a system command,  and provide 
    --option completions for it (from previous invocations)
    
    Note that this only works with sh profile
    """
    
    global _syscmds
    
    if '$' in event.line:
        # It may have itpl templates
        raise IPython.ipapi.TryNext
    
    db = ip.db

    if _syscmds is None:
        try:
            _syscmds = set(db.get("syscmdlist",[] ) )
        except KeyError:
            raise IPython.ipapi.TryNext
                         
    cmd = os.path.basename(event.command)
    
    if not (cmd.startswith('!') or cmd in _syscmds or os.access(event.command,  os.X_OK)):
        raise IPython.ipapi.TryNext
        
    cmd = cmd.lstrip('!')
    sym = event.symbol
    
    if sym.startswith('-'):
        #print "is option", sym
        opts = list(db.get('syscmdopt/' + cmd,  []))
        #print "has opts",  opts
        return opts

    fm = ip.IP.Completer.file_matches(event.symbol)
    #print "filematch", fm
    return fm
    
ip.set_hook('complete_command',  shellcmd_completer,  re_key = ".*")    

def store_options_hook(self,  cmdline):
    """ Stores all the -options and --options for executed system commands """
    #print "shellhook", cmdline
    opts = set(s.rstrip(',:') for s in cmdline.split() if s.startswith('-'))
    if not opts:
        raise IPython.ipapi.TryNext
    db = ip.db
    cmd = cmdline.split(None,  1)[0]
    cmd = os.path.basename(cmd)
    dbloc = 'syscmdopt/' + cmd
    stored = db.get(  dbloc,  set())
    stored.update(opts)
    db[dbloc] = stored
    #print "added",  stored
    raise IPython.ipapi.TryNext

ip.set_hook('shell_hook',  store_options_hook,  priority = 1)        

def options_remember_f(self,  args):
    """ Call with system command --help, -h, -? or whatever to cache options
    
    Used for system command completer
    
    E.g. 
    
    %options_remember ./configure --help
    %options_rememer zip -h
    
    After invocation, the tab completer suggests the options
    found when you do e.g. cp -<tab>
    """

    parts = args.split(None,  1)
    cmd = os.path.basename(parts[0])
    out = os.popen(args).read()
    print out
    opts = list(set(s for s in out.split() if s.startswith('-')))    
    opts.sort()
    if not opts:
        print "No options found"
        return
    print "Command:", cmd
    print "Adding options:"
    print opts
    db = ip.db
    dbloc = 'syscmdopt/' + cmd
    stored = db.get(  dbloc,  set())
    stored.update(opts)
    db[dbloc] = stored

ip.expose_magic('options_remember',  options_remember_f)

        
    
    
