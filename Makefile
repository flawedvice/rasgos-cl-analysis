files := requeriments.txt herbario.py analysis.ipynb analysis.pdf analysis.html .gitignore data/herbario_species.csv data/species_names.csv
zipname := analysis.zip

all: clean zip

zip:
	zip $(zipname) $(files)

clean:
	rm -f $(zipname)