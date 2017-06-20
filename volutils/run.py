"""
This is for running in batch python methods as commands
Based on Rob Carver code
"""

import inspect
from yaml import load as yload
import importlib
import sys
from volsetup import config

globalconf = config.GlobalConfig()
config_file = globalconf.config['paths']['config_folder'] + "commandlist.yaml"

TRACE = False

REPL = True


def get_all_config():
    """
    Get all configuration
    """
    try:
        with open(config_file,'r') as f:
            all_config_info=yload(f)
    except IOError: # doesnt work on 2.7 FileNotFoundError:
        raise Exception("Need a valid yaml file as the configuration, %s didn't work" % config_file)
    return all_config_info
    
def get_config(funcname):
    """
    Return config information for a particular function
    """
    all_config_info=get_all_config()
    try:
        config_info=all_config_info[funcname]
    except KeyError:
        raise Exception("Function %s not found in config file %s" % (funcname, config_file))

    ## Full pointer to where a module is with "." seperation eg demofunc.demofunc
    full_funcname=config_info['pointer']
    
    ## Optional type casting
    type_casting=config_info.get("typecast", dict())
        
    return full_funcname, type_casting
    
def fill_args_and_run_func(func, full_funcname, type_casting=dict(),args1=None):
    """
    Prints the docstring of func, then asks for all of its arguments with defaults
    
    Optionally casts to type, if any argument name is an entry in the dict type_casting
    """
    ##print doc string
    if TRACE:
        print(func)
        print("Doc string for %s" % full_funcname)

    #print("\n")
    print(inspect.getdoc(func))
    #print("\n")
    func_arguments=inspect.getargspec(func).args
    func_defaults=inspect.getargspec(func).defaults
    
    
    func_arguments=list(func_arguments)
    
    if func_defaults is None:
        func_defaults=list()
    else:
        func_defaults=list(func_defaults)
    
    count_star_args=len(func_arguments) - len(func_defaults)
    
    func_defaults= [None]*count_star_args + func_defaults   
    
    def _fshow(argname, argdefault):
        if argdefault is None:
            return argname
        else:
            return argname+"="+str(argdefault)
        
    print("Arguments:")
    print([_fshow(argname, argdefault) for (argname, argdefault) in zip(func_arguments, func_defaults)])
    #print("\n")

    args=[]
    kwargs=dict()

    if REPL and (args1 is not None):
        #print("REPL %s" % (str(args1)) )
        for idx, x in enumerate(func_arguments):
            type_to_cast_to=type_casting.get(x,None)
            if type_to_cast_to is not None:
                try:
                    ## Cast the type
                    type_func = eval("%s" % type_to_cast_to)
                    args1[idx] = type_func(args1[idx])
                except:
                    print("Couldn't cast value %s to type %s: retype or check %s\n" %
                          (args1[idx], type_to_cast_to, config_file))

        args.extend(args1)

    if TRACE:
        print("\nRunning %s() with args %s, kwargs %s\n" % (full_funcname, args, kwargs))

    #print("\n")
    if (args1 is not None) or (not func_arguments):
        func(*args, **kwargs)

    #print("Finished")


if __name__ == '__main__':
    #Load the function and config information
    #Requires a single argument, name of function reference in file
    if len(sys.argv)==1:
        print("Enter the name of a function located in %s" % config_file)
        all_config_data=list(get_all_config().keys())
        print("Any one from:")
        for x in all_config_data:
            full_funcname, type_casting=get_config(x)
            funcsource, funcname = full_funcname.rsplit('.', 1)
            mod = importlib.import_module(funcsource)
            func = getattr(mod, funcname, None)
            print((x,str(inspect.getdoc(func))))
        print("Example . p %s" % all_config_data[0])
        
        exit()

    ## From the config file get the full pointer and    
    func_reference_name=sys.argv[1]
    full_funcname, type_casting=get_config(func_reference_name)
    
    ## full_funcname is something like demofunc.demofunc2
    ## type_castig is an optional dict, keys are argument names, entries are types eg dict(arg1="int")
    
    funcsource, funcname = full_funcname.rsplit('.', 1)

    ## stop overzealous interpreter tripping up
    func=None

    if TRACE:
        print("Running %s imported from %s" % (funcname, funcsource))

    args1 = None
    if REPL and ( len(sys.argv) > 2 ) :
        #print("REPL arguments passed %s" % ( str(sys.argv[2:]) ))
        args1=sys.argv[2:]

    ## imports have to be done in main
    try:
        mod = importlib.import_module(funcsource)
    except ImportError:
        raise Exception("NOT FOUND: Module %s specified for function reference %s\n Check file %s" % (funcsource, func_reference_name, config_file))
    
    func = getattr(mod, funcname, None)
    
    if func is None:
        raise Exception("NOT FOUND: function %s in module %s  specified for function reference %s \n Check file %s" % (funcname, mod, func_reference_name, config_file))
        
    fill_args_and_run_func(func, full_funcname, type_casting,args1=args1)
