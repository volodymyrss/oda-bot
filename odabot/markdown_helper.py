import os
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from markdown import markdown
from markdown.postprocessors import Postprocessor
from markdown.extensions import Extension

img_src_pattern = r'(?P<pref><img[^>]*\bsrc\s*=\s*["\'])(?P<url>.*?)(?P<suff>["\'][^>]*\/{0,1}>)'

def parse_url(url):
    scheme, netloc, path, query, fragment = urlsplit(url)
    
    is_rel_path = True
    if scheme != '':
        is_rel_path = False
    elif url.startswith('//') or scheme == 'file':
        is_rel_path = False
    elif path == '' and netloc == '':
        # url fragment? can't be used as path
        is_rel_path = False
    
    return scheme, netloc, path, query, fragment, is_rel_path

def append_url_base(m, url_base):
    link = m.group(0)
    
    scheme, netloc, path, query, fragment, is_relative = parse_url(m.group('url'))
    
    if is_relative:
        base_spl = urlsplit(url_base)
        
        new_path = os.path.normpath(os.path.join(base_spl.path, path))
        
        new_url = base_spl._replace(path=new_path).geturl()
        
        link = f"{m.group('pref')}{new_url}{m.group('suff')}"
    
    return link
    

class ImgBasePostprocessor(Postprocessor):
    def run(self, text):
        
        url_base = self.config['url_base']
        
        text = re.sub(img_src_pattern, lambda m: append_url_base(m, url_base), text)
        
        return text
        
class ImgBase(Extension):
    def __init__(self, **kwargs: Any) -> None:
        self.config = {'url_base': ["", "Base to prepend to image pathes"]}
        super().__init__(**kwargs)
    
    def extendMarkdown(self, md):
        ibpp = ImgBasePostprocessor(md)
        ibpp.config = self.getConfigs()
        
        md.postprocessors.register(ibpp, 'imgbase', 2)
   

def convert_help(text_md, url_base=''):
    text_html = markdown(text_md, 
                         extensions=['attr_list', 'markdown_katex', 
                                     ImgBase(url_base=url_base)], 
                         extension_configs = {'markdown_katex': {'insert_fonts_css': True}})
    return text_html
