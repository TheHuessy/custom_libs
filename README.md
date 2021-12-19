This structure works, but we may need to look into a better way to combine everything. With this example, from `custom_libs` we have to run `from test import test` and then `test.TestClass()`.

Ideally we'd have it so that it would just be an `import custom_libs` or `from custom_libs import [ClassName]`
We can do this just by having each class be its own file within `custom_libs`.
