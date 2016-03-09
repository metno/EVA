# EVA
This daemon listens for messages coming from productstatus and
submits jobs to, e.g., gridengine based on these incoming messages.

Testing: 
```
python setup.py develop
EVA_ADAPTER=eva.adapter.NullAdapter python -m eva
```
