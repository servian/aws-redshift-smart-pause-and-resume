class Error(Exception):
    """Base class for other exceptions"""
    pass
 
class ResourceCreateInProgressException(Error):
    """Resource Create In Progress Exception"""
    pass

class ResourceInFailedStateException(Error):
    """Resource In Failed State Exception"""
    pass