### Part 1

You can use 10 trees for both the models. 

We expect you to achieve
    * ~20% test error(or better) for CoverType
    * ~30% test error(or better) for Higgs dataset.
    
### Part 2

Remove all the plotting commands when running code on PyBolt.

Datasets location to be used on PyBolt:
    * /covtype/covtype.data
    * /HIGGS/HIGGS.csv

#### CoverType dataset

Use **full** dataset. Make the problem binary as done in notebook.

Splitting should be done using:

    (trainingData,testData)=Data.randomSplit([0.7,0.3],seed=255)

* Expected test error <= 13%
* Expected running time <= 350 seconds


#### Higgs dataset

Use only **10%** data. Sample using:

    input_sampled = inputRDD.sample(False,0.1, seed=255)

Splitting should be done using:

    (trainingData,testData)=Data.randomSplit([0.7,0.3],seed=255)

* Expected test error <= 27.5%
* Expected running time <= 350 seconds

### final submission

submit a zip file containing:
    * 2 notebooks (.ipynb) for part 1
    * 2 python files (which you would run on PyBolt) for part 2

