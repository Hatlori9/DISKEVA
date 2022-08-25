import macro_define



class userrequest_type:
    def __init__(self):
        self.length = 199
        self.video_id = 0
        self.features = ["cat", "bird", "dog", "rabbit", "tiger", "fish"]
        self.features_size1 = 0
        self.features_size2 = 0
        self.operation = "AND"
        self.arrival_time = 0
        self.finish_time = 0
        self.generate_time = 0
        self.max_feature_size = 0
        self.priority = macro_define.HIGH_PRIORITY

    def get_copy():
        newcopy = userrequest_type()

        return newcopy

    def __lt__(self, other):
        return self.priority > other.priority

