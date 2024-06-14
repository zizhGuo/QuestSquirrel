def process_query(query):
    import re
    query = query.replace('\n', ' ')  # Remove newline characters
    query = re.sub(r'\s+', ' ', query)  # Replace multiple whitespaces with a single whitespace
    # query = re.sub('`', r'\`', query)
    return query

def fill_template(query:str, parameters: dict):
    return query.format(**parameters)