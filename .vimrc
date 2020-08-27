syntax enable
syntax on

set paste
" tab configuration
set expandtab
set smarttab
set shiftwidth=4
set tabstop=4
"set ai
" " set cindent shiftwidth=2
" enable mouse scroll

set hlsearch
set nocompatible
"set backspace=indent,eol,start
"set encoding=chinese
"set langmenu=zh_CN.GBK
set imcmdline
source $VIMRUNTIME/delmenu.vim
source $VIMRUNTIME/menu.vim
"set number
colorscheme desert

autocmd BufNewFile *.sh 0r ~/.vim/template/shell/shellconfig.sh
autocmd BufNewFile *.py 0r ~/.vim/template/python/pythonconfig.py
au BufNewFile,BufRead *.hql set filetype=hive expandtab
