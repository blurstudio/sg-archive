import json
from datetime import datetime


class DateTimeDecoder(json.JSONDecoder):
    """https://gist.github.com/abhinav-upadhyay/5300137"""

    def __init__(self, *args, **kargs):
        if "object_hook" not in kargs:
            kargs["object_hook"] = self.dict_to_object
        super(DateTimeDecoder, self).__init__(*args, **kargs)

    def dict_to_object(self, d):
        if "__type__" not in d:
            return d

        _type = d.pop("__type__")
        try:
            dateobj = datetime(**d)
            return dateobj
        except Exception:
            d["__type__"] = _type
            return d


class DateTimeEncoder(json.JSONEncoder):
    """Instead of letting the default encoder convert datetime to string,
    convert datetime objects into a dict, which can be decoded by the
    DateTimeDecoder
    https://gist.github.com/abhinav-upadhyay/5300137
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                "__type__": "datetime",
                "year": obj.year,
                "month": obj.month,
                "day": obj.day,
                "hour": obj.hour,
                "minute": obj.minute,
                "second": obj.second,
                "microsecond": obj.microsecond,
            }
        else:
            return json.JSONEncoder.default(self, obj)
