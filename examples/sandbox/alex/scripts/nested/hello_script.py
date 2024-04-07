import cdf

w = cdf.get_workspace(__file__).unwrap()

print(f"Hello, world from {w.name}!")
