import cdf

w = cdf.get_workspace_from_path(__file__).unwrap()

print(f"Hello, world from {w.name}!")
