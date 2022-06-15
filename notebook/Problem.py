# Databricks notebook source
# MAGIC %md # Problem
# MAGIC 
# MAGIC Simple test notebook to demonstrate the problem of calling custom Python code from a UDF, where the custom Python code is not in the default `sys.path`.
# MAGIC 
# MAGIC This demonstration notebook assumes that you've checked-out the sample Git repo into your home directory:
# MAGIC * https://github.com/aaronbinns/db-repo-path
# MAGIC 
# MAGIC This test notebook can be run on any cluster with at least 1 worker node.  There's no special cluster config required, nor does it require any particular DBR version.

# COMMAND ----------

# MAGIC %md ## Test 1 - Call custom Python module when running on Driver node
# MAGIC 
# MAGIC Since we don't update the `sys.path` first, Python cannot find our custom module `greetings.hello`

# COMMAND ----------

print( greetings.hello.hello_world() )

# COMMAND ----------

# MAGIC %md ## Test 2 - Call custom Python module when running on Driver node
# MAGIC 
# MAGIC By adding the full path to the `db-repo-path` repository's subdirectory `src` to the `sys.path`, the Python interpreter running on the Driver node can successfully call our custom module `greetings.hello`

# COMMAND ----------

import sys

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
sys.path.append( f"/Workspace/Repos/{username}/db-repo-path/src" )

# COMMAND ----------

import greetings.hello

print( greetings.hello.hello_world() )

# COMMAND ----------

# MAGIC %md ## Test 3 - Call custom Python module when running on Worker node
# MAGIC 
# MAGIC Create a trivial Python UDF to call a function in the `greetings.hello` module.
# MAGIC 
# MAGIC This fails because the Python environment on the worker node does not have the path to our custom module in its `sys.path`

# COMMAND ----------

def test_import(unused):
    import greetings.hello
    return greetings.hello.hello_world()
    
sc.parallelize(["foo","bar","baz","frotz","bizzle","fizzle"]).map(test_import).collect()

# COMMAND ----------

# MAGIC %md ## Test 4 - Call custom Python module when running on Worker node
# MAGIC 
# MAGIC This cell demonstrates the current recommendation for dealing with this problem: modify the `sys.path` inside the UDF itself.  Since the UDF runs on the worker node, modification to the `sys.path` will be performed on the Python environment on the worker node.
# MAGIC 
# MAGIC Note that this modification will only persist for the UDF call stack.  Once this UDF runs, the worker node's Python environment will no longer have the modified `sys.path`.
# MAGIC 
# MAGIC So, if you had, say 100 different UDFs, then you'd have to modify each and every one of them to update the `sys.path` every time they are run.

# COMMAND ----------

def test_import(unused):
    import sys
    sys.path.append( f"/Workspace/Repos/{username}/db-repo-path/src" )

    import greetings.hello
    return greetings.hello.hello_world()
    
sc.parallelize(["foo","bar","baz","frotz","bizzle","fizzle"]).map(test_import).collect()

# COMMAND ----------

# Even though we run this right after the previous cell, since we didn't update the `sys.path` again in this UDF, 
# the worker node's Python environment can't find the `greetings.hello` module.
def test_import2(unused):
    import greetings.hello
    return greetings.hello.hello_world()
    
sc.parallelize(["foo","bar","baz","frotz","bizzle","fizzle"]).map(test_import2).collect()

# COMMAND ----------



# COMMAND ----------


