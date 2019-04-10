#Removes special characters from the Census data

################################################################################
## Variables/Constatns
################################################################################

bgFiles = ('2009.txt', '2013.txt', '2014.txt', '2015.txt', '2016.txt', '2017.txt')
tractFiles = ('2009.txt','2010.txt','2011.txt','2012.txt', '2013.txt', '2014.txt', '2015.txt', '2016.txt', '2017.txt')

################################################################################
## Functions
################################################################################

def cleanFile(inputFileName, outputFileName):
    """
    Removes special characters from the datasets
    """

    with open(r'' + inputFileName, 'r') as infile, \
         open(r'' + outputFileName, 'w') as outfile:
         data = infile.read()
         data = data.replace("],", "")
         data = data.replace("[", "")
         data = data.replace("\"", "")
         outfile.write(data)

def main():
    """
    Main execution function to perform required actions
    """

    for x in range(len(bgFiles)):
        inputFile = 'BlockGroup/' + bgFiles[x]
        outputFile = 'Clean/BlockGroupClean/' + bgFiles[x]
        cleanFile(inputFile, outputFile)

    for x in range(len(tractFiles)):
        inputFile = 'Tract/' + tractFiles[x]
        outputFile = 'Clean/TractClean/' + tractFiles[x]
        cleanFile(inputFile, outputFile)

################################################################################
## Execution
################################################################################

if __name__ == '__main__':
    main()
