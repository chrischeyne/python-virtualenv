""" Gmail interface for IPython

LICENSE: This extension is licensed under GPL 2.0  (i.e. the same license as
libgmail)

This extension requires libgmail (http://libgmail.sourceforge.net/, 
apt-gettable on ubuntu), and is mostly provided for entertainment purposes .

Usage:

Log in by doing::

    import ipy_gmail
    ipy_gmai.set_account('myusername','mypassword')

 (this only has to be done once and will be remembered across ipython sessions)
 
 After that, you can do gmi (inbox), gms foo bar (search), gmu (unread) 
 and other things (to be documented).
 
 Example of actually reading an email:
 
 [~]|749> gms englische
   <749>
a: <T 'Englische Bucher: 5 EUR Rabatt'>
b: <T '\u003cb\u003eBuch-Empfehlungen fur Sie\u003c/b\u003e'>
c: <T 'Buch-Restposten ab 2,95 EUR'>
d: <T 'Die besten Preis-Hits und Neuheiten bei Amazon.de'>
[~]|750> m = _.a.g
[~]|751> print m.source

Note how you specify the list index with alphabetical characters, and use .g to Get the first mail in thread (it's a shortcut). The search itself shows the threads.
 
"""

import libgmail
import email


import IPython.ipapi
ip = IPython.ipapi.get()

alphahex_base = ord('a')
def alphahex(num):
    h = hex(num)

    return ''.join(chr(alphahex_base + int(ch, 16)) for ch in h[2:])

def alphahex2int(ahex):
    h = ''.join( hex( ord(ch) - alphahex_base) [ 2 ] for ch in ahex)
    return int(h, 16)

class AnnotatedList(list):
    """ Show indices in repr """
    def __repr__(self):
        els = []

        for i, s in enumerate(self):
            els.append('%s: %s' % (alphahex(i),repr(s)))
        return "\n".join(els)
    def __getattr__(self, name):
        """ Access through i12 syntax 
        
        (easier to type than i[12])
        """
        return self[alphahex2int(name)]
        
def gt_repr(self):
    return "<T '%s'>" % self.subject

def gt_getall(self):
 	
    for msg in self:
	mim = email.message_from_string(msg.source)
	if not mim.is_multipart():
	    yield mim.get_payload()
	    continue
	for p in mim.get_payload():
	    if p.get_content_type() == 'text/plain':
		yield p.get_payload()
		break
    return


def gt_getfirst(self):
    mim = email.message_from_string(self[0].source)
    if not mim.is_multipart():
	return mim.get_payload()
    for p in mim:
	if p.get_content_type() == 'text/plain':
	    return p.get_payload()

libgmail.GmailThread.__repr__ = gt_repr
libgmail.GmailThread.g = property(gt_getall)

def set_account(username,  passwd):
    """ This stores the password in files system as plain text
    
    I.e. it's inherently insecure    
    """
    ip.db['gmail/account'] = username
    ip.db['gmail/password'] = passwd

_ga = None

def acct():
    if _ga: 
        return _ga
    connect()
    return _ga

def connect():
    global _ga
    _ga = libgmail.GmailAccount(ip.db['gmail/account'],  ip.db['gmail/password'])
    _ga.login()
    return _ga

def gms_f(self, s):
    """ Gmail search
    
    Example::
      %gms foo
    """
    return AnnotatedList(acct().getMessagesByQuery(s))

ip.expose_magic('gms',  gms_f)

def gmf_f(self,  folder):
    """ Get messages by folder """
    return AnnotatedList(acct().getMessagesByFolder(folder))    

ip.expose_magic('gmf',  gmf_f)

def gml_f(self, label):
    """ get by label """
    return AnnotatedList(acct().getMessagesByLabel(label))    
ip.expose_magic('gml',  gml_f)    
    

def gmu(self):
    """ List unread messages
    """
    return AnnotatedList(acct().getUnreadMessages())    

def gmi():
    """ Inbox """
    return gmf_f(None, 'inbox')

def gmstore_f(self,arg):
    """ store file in gmail (drafts) """
    acct().storeFile(arg)

ip.expose_magic('gmstore', gmstore_f)


ip.to_user_ns("gmi gmu")    
    
def to_leo(threads, rootNode = None):
    wb = ip.user_ns['wb']    
    if rootNode is None:
	root = wb.MailImport
    else:
	root = rootNode
	
    for idx,  thread in enumerate(threads):
        key = '%d %s' % (idx,  thread.subject)
        root[key] = None
        th = root[key]
        print th
        print thread
	try:
	    msgs = list(thread.g)
	except TypeError:
	    th.b = "ipy_gmail: Import failed"
	
        if len(msgs) == 1:
            th.b = msgs[0]
	else:
	    for m in msgs:
		head = m.split('\n',1)[0]
		n = th.append()
		n.h = head
		n.b = m
    root.go()
