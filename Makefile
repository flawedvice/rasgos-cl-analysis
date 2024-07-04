files := requeriments.txt herbario.py analysis.ipynb .gitignore data/herbario_species.csv data/species_names.csv
zipname := analysis.zip

all: clean zip

zip:
	zip $(zipname) $(files)

clean:
	rm -f $(zipname)