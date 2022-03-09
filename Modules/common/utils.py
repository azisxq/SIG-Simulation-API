def validate(arg, key, expected_type, default_value):
    try:
        if expected_type == list:
            return arg.getlist(key+'[]')
        val = arg.get(key)
        if val is None:
            return default_value
        payload = expected_type(val)
        return payload
    except Exception as e:
        return default_value