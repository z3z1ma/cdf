"""
Generate a sandbox in the filesystem in which to run code
It should update sys path to include the sandbox
And it should clean up the sandbox when done
Furthermore it should take all user code and put it in the sandbox
such that imports work correctly throughout the sandbox which is
analagous to a cdf workspace, and futher to a python package








OK it gets more interesting

1. We want to be able to get the "source" of a pipeline script that is being passed into
the pipline.exract method. 

So we need to dynamically inject an importable module into the sandbox
that contains hooks which will convert pipeline.extract into a method
that simply prints the resources 

Our rewriter already replaces the pipeline constructor with our own constructor.
So presumably this mechanism can be leveraged for this. Perhaps like this?


from cdf.pipeline import echo_pipeline, head_pipeline, debug_pipeline?

and `rewriter` will replace `dlt.pipeline` with `echo_pipeline` or `head_pipeline` or `debug_pipeline`

One consideration is that user may do 1 of 2 entrypoints,
pipeline.run or pipeline.extract.

pipeline.run will ultimately call pipeline.extract, so we can just replace pipeline.extract

we also need to throw a catchable exception I think as a primitive "go to" mechanism
to jump back out. In fact the exception can carry back a value which is another damn good idea.

Maybe the Exception is actually the genious idea here. We can use it to carry back the value
containing whatever tf we want.
"""
