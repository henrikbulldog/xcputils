""" Checkpoints """

class Checkpoint:
    """ Checkpoint base class """

    def get(self, default: str = None) -> str:
        """ Get checkpoint """

    def set(self, value: str):
        """ Set checkpoint """

    def reset(self):
        """ Reset checkpoint """
        self.set("")        
