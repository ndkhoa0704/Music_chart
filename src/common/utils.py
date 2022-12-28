import re


def add_url_params(url: str, params):
    '''
    Construct url with parameters
    '''
    if not '?' in url:
        url += '?'
    for key in params.keys():
        url += f'{key}={params[key]}&'
    return url[:-1]
            
            


def json_path(path: str, json_data: dict):
    '''
    Get item from json data based on predefined path
    '''
    # if not isinstance(json_data, dict) or isinstance(json_data, list):
    #     raise Exception('Not a valid json')
    if path is None: # End recursion
        return json_data
    
    dot_idx = path.find('.')
    if dot_idx == -1:
        next_path = None
        item = path
    else:
        next_path = path[dot_idx + 1:]
        item = path[:dot_idx]
    del dot_idx

    if isinstance(json_data, list):
        in_brackets = next(re.finditer(r'\[(.+)\]', item), None)
        if in_brackets is None:
            raise Exception('Not a valid path')
        in_brackets = in_brackets.group(1)
        if in_brackets == '*':
            return [json_path(next_path, i) for i in json_data]
        elif re.match(r'\d*:\d*', in_brackets):
            return [json_path(next_path, i) for i in json_data]
        elif not in_brackets.isdigit():
            raise Exception('Not a valid path')
        else:
            in_brackets = int(in_brackets)
        json_data = json_data[in_brackets]
    else:
        json_data = json_data.get(item)
        if json_data is None:
            return None
    return json_path(next_path, json_data)