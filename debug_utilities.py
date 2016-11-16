#!/usr/bin/env python3
from datetime import date

def debug_print(is_debug,use_file, args):
    if isdebug:
        if use_file:
            with open("log"+date.today().strftime('%Y_%d_%m_%h')+".txt", "a") as myfile:
                myfile.write(args)
        else:
            print(args)
