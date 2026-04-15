#!/bin/python

import subprocess



def greet():
    print( "\nWelcome to App Checker")
    print( "\nCheck and fix duplicate , and dead imports, unused variable removal... " )
    print("\nRunning Tests and correcting errors ...\n")
    
    
if __name__ == '__main__':
    greet()
    result = subprocess.run( ["uvx", "ruff","check"] )
    if result.returncode == 0:
        print(f"\n{result}")
    else:
        result = subprocess.run( ["uvx", "ruff","check", "--fix"] )        
        #subprocess.run( "exit 1", shell=True, check=True )
        print( f"\n{result}" )
    
