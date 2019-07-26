# Load local packages
import repec
import remotes

def update():
    '''Run full database update'''
    repec.update()
    remotes.update()

# update()
