'''
Data as it travels through the processors needs some minimal structure
to keep the identity of the original data
'''


class Data(object):

    id = None
    payload = None

    def __init__(self, id, payload):
        self.id = id
        self.payload = payload
