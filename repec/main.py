# Load local packages
import repec
import remotes
import papers

def update():
    '''Run full database update'''
    repec.update()
    remotes.update()
    papers.update()

# update()
