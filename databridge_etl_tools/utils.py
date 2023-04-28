import click

def pass_params_to_ctx(context: 'click.Context', **kwargs): 
    '''Take a context and assign kwargs to it to be passed to child commands'''
    for param, value in kwargs.items(): 
        context.obj[param] = value
    return context
