# Initial Project Work
When scoping out the API I used marimo and not Jupyter Notebooks to test out my functions and see results.

Using uv to manage my projects I could install marimo with the following:
```
uv add marimo
```

To then run marimo you can use the following:
```
uv run marimo edit supplier_check.py
```
Or can simply run it as a script with the following:
```
uv run test.py
```

This gives far more flexibility on my initial work then running a jupyter notebook. Allows me to manage dependencies across my inital part of the project and main project with uv and flexibility on testing data outputs.