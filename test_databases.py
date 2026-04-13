import os
import sys

import utils
import connection

print("Imported utils and connection")
print("Index:", utils._find_col(["created_on", "name", "is_default"], "name"))
print("Index:", utils._find_col(["CREATED_ON", "NAME", "IS_DEFAULT"], "name"))
print("Index:", utils._find_col(["created_on", "NAME", "is_default"], "NAME"))
