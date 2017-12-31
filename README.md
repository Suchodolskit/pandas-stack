# pandas-stack
<b>pandas-stack</b> is a script that converts stack exchange xmls files to pandas data frames and serializes those data frames to pickle format.

If you are interested in analyzing stack exchange data, you can download the xml files from https://archive.org/details/stackexchange. For instance, after you download the dataset and extract it to a directory named ```stack-data```, you only need to call:

```
python pandas-stack.py stack-data nro_processes
```

where ```nro_processes``` is the number of processes used to convert the xmls (each xml is converted by one process; since each stack exchange site has more than one xml (table), it may be faster to use more than one process). You should note that:
- ```stack-data``` may contain more than one stack exchange directory.
- data frames serialized in pickle format are saved alongside xmls.

Finally, in order to run <b>pandas-stack</b> you will have to install:

- Python3
- Argparse
- bs4
- Pandas
- Pickle

which can be easily accomplished by using pip and anaconda.
