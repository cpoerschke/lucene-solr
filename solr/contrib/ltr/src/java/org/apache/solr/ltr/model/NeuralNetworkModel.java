/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.ltr.model;

import java.lang.Math;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A scoring model that computes document scores using a neural network.
 * <p>
 * Example configuration:
<pre>{
    "class" : "org.apache.solr.ltr.model.NeuralNetworkModel",
    "name" : "rankNetModel",
    "features" : [
        { "name" : "documentRecency" },
        { "name" : "isBook" },
        { "name" : "originalScore" }
    ],
    "params" : {
        "inputs" : [
            "documentRecency",
            "isBook",
            "originalScore"
        ],
        "layers" : [
            [
                { "weights" : [  1.0,  2.0,  3.0 ], "bias" :  4.0 },
                { "weights" : [  5.0,  6.0,  7.0 ], "bias" :  8.0 },
                { "weights" : [  9.0, 10.0, 11.0 ], "bias" : 12.0 },
                { "weights" : [ 13.0, 14.0, 15.0 ], "bias" : 16.0 }
            ],
            [
                { "weights" : [ 13.0, 14.0, 15.0, 16.0 ], "bias" : 17.0 },
                { "weights" : [ 18.0, 19.0, 20.0, 21.0 ], "bias" : 22.0 }
            ]
        ],
        "output" : { "weights" : [ 23.0, 24.0 ], "bias" : 25.0 }
        "weights" : [
            [ [ 1.0, 2.0, 3.0, 4.0 ], [ 5.0, 6.0, 7.0, 8.0 ], [ 9.0, 10.0, 11.0, 12.0 ], [ 13.0, 14.0, 15.0, 16.0 ] ],
            [ [ 13.0, 14.0, 15.0, 16.0, 17.0 ], [ 18.0, 19.0, 20.0, 21.0, 22.0 ] ],
            [ [ 23.0, 24.0, 25.0 ] ]
        ],
        "nonlinearity": "relu"
    }
}</pre>
 * <p>
 * Training libraries:
 * <ul>
 * <li> <a href="https://github.com/airalcorn2/RankNet">Keras Implementation of RankNet</a>
 * </ul>
 * <p>
 * Background reading:
 * <ul>
 * <li> <a href="http://icml.cc/2015/wp-content/uploads/2015/06/icml_ranking.pdf">
 * C. Burges, T. Shaked, E. Renshaw, A. Lazier, M. Deeds, N. Hamilton, and G. Hullender. Learning to Rank Using Gradient Descent.
 * Proceedings of the 22nd International Conference on Machine Learning (ICML), ACM, 2005.</a>
 * </ul>
 */
public class NeuralNetworkModel extends LTRScoringModel {

  private List<String> inputFeatureNames;
  private int[] inputFeatureIndices;
  private List<NeuralNetworkLayer> layers;
  private NeuralNetworkNode output;
  protected ArrayList<float[][]> weightMatrices;
  protected String nonlinearity;

  public class NeuralNetworkLayer {

    final private List<NeuralNetworkNode> nodes;

    public NeuralNetworkLayer(List<NeuralNetworkNode> nodes) {
      this.nodes = nodes;
    }

    /**
     * Check the validity of this layer's nodes.
     * @param numInputs - number of inputs for nodes in this layer
     * @return number of outputs from this layer
     */
    public int validate(int numInputs) throws ModelException {
      for (NeuralNetworkNode node : nodes) {
        node.validate(numInputs);
      }
      return nodes.size();
    }

    public float[] calculateOutputs(float[] inputs) {
      final float[] outputs = new float[nodes.size()];
      for (int ii=0; ii<outputs.length; ++ii) {
        outputs[ii] = doNonlinearity(nodes.get(ii).calculateOutput(inputs));
      }
      return inputs;
    }

  }

  public class NeuralNetworkNode {

    private float[] weights;
    private float bias;

    public NeuralNetworkNode(Map<String,Object> obj) {
      SolrPluginUtils.invokeSetters(this, obj.entrySet());
    }

    public void validate(int numInputs) throws ModelException {
      if (weights.length != numInputs) {
        throw new ModelException("Too many or too few weights or inputs:"
            + " weights.length="+Integer.toString(weights.length)
            + " numInputs="+Integer.toString(numInputs));
      }
    }

    public void setWeights(Object obj) {
      final List<Float> weightsList = (List<Float>) obj;
      this.weights = new float[weightsList.size()];
      for (int ii=0; ii<weights.length; ++ii) {
        this.weights[ii] = weightsList.get(ii);
      }
    }

    public void setBias(float bias) {
      this.bias = bias;
    }

    public float calculateOutput(float[] inputs) {
        float output = bias;
        for (int ii = 0; ii < weights.length; ii++) {
          output += inputs[ii] * weights[ii];
        }
        return output;
    }

  }

  public void setInputs(Object inputs) {
    // map features' names to indices
    final HashMap<String,Integer> featureName2featureIndex = new HashMap<String,Integer>();
    for (int i = 0; i < features.size(); ++i) {
      final String key = features.get(i).getName();
      featureName2featureIndex.put(key, i);
    }
    // note: a feature name could appear multiple times in the inputs
    inputFeatureNames = (List<String>) inputs;
    inputFeatureIndices = new int[inputFeatureNames.size()];
    for (int ii=0; ii<inputFeatureIndices.length; ++ii) {
      final Integer idx = featureName2featureIndex.get(inputFeatureNames.get(ii));
      if (idx != null) {
        inputFeatureIndices[ii] = idx.intValue();
      } else {
        inputFeatureIndices[ii] = -1; // unknown feature
      }
    }
  }

  public void setLayers(Object listOfListsObj) {
    layers = new ArrayList<NeuralNetworkLayer>();
    for (final List<Object> listOfObjects : (List<List<Object>>) listOfListsObj) {

      final List<NeuralNetworkNode> nodes = new ArrayList<NeuralNetworkNode>();
      for (final Object obj : listOfObjects) {

        nodes.add(new NeuralNetworkNode((Map<String,Object>)obj));
      }

      layers.add(new NeuralNetworkLayer(nodes));
    }
  }

  public void setOutput(Object obj) {
    this.output = new NeuralNetworkNode((Map<String,Object>)obj);
  }

  public void setWeights(Object weights) {
    final List<List<List<Double>>> matrixList = (List<List<List<Double>>>) weights;

    weightMatrices = new ArrayList<float[][]>();

    for (List<List<Double>> matrix : matrixList) {
      int numRows = matrix.size();
      int numCols = matrix.get(0).size();;

      float[][] weightMatrix = new float[numRows][numCols];

      for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < numCols; j++) {
          weightMatrix[i][j] = matrix.get(i).get(j).floatValue();
        }
      }

      weightMatrices.add(weightMatrix);
    }
  }

  public void setNonlinearity(Object nonlinearityStr) {
    nonlinearity = (String) nonlinearityStr;
  }

  private float[] dot(float[][] matrix, float[] inputVec) {

    int matrixRows = matrix.length;
    int matrixCols = matrix[0].length;
    float[] outputVec = new float[matrixRows];

    for (int i = 0; i < matrixRows; i++) {
      float outputVal = matrix[i][matrixCols - 1]; // Bias.
      for (int j = 0; j < matrixCols - 1; j++) {
        outputVal += matrix[i][j] * inputVec[j];
      }
      outputVec[i] = outputVal;
    }

    return outputVec;
  }

  private float doNonlinearity(float x) {
    if (nonlinearity.equals("relu")) {
      return x < 0 ? 0 : x;
    } else {
      return (float) (1 / (1 + Math.exp(-x)));
    }
  }

  protected float doNonlinearity(String nonlinearity, float x) {
    if (nonlinearity.equals("relu")) {
      return x < 0 ? 0 : x;
    } else if (nonlinearity.equals("sigmoid")) {
      return (float) (1 / (1 + Math.exp(-x)));
    } else if (nonlinearity.equals("identity")) {
      return x;
    } else {
      // should never get here
      return 0;
    }
  }

  public NeuralNetworkModel(String name, List<Feature> features,
                 List<Normalizer> norms,
                 String featureStoreName, List<Feature> allFeatures,
                 Map<String,Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  @Override
  protected void validate() throws ModelException {
    super.validate();

    if (!nonlinearity.matches("relu|sigmoid")) {
      throw new ModelException("Invalid nonlinearity for model " + name + ". " +
                               "\"" + nonlinearity + "\" is not \"relu\" or \"sigmoid\".");
    }

    int inputDim = features.size();

    for (int i = 0; i < weightMatrices.size(); i++) {
      float[][] weightMatrix = weightMatrices.get(i);

      int numRows = weightMatrix.length;
      int numCols = weightMatrix[0].length;

      if (inputDim != numCols - 1) {
        if (i == 0) {
          throw new ModelException("Dimension mismatch. Input for model " + name + " has " + Integer.toString(inputDim)
                                   + " features, but matrix #0 has " + Integer.toString(numCols - 1) +
                                   " non-bias columns.");
        } else {
          throw new ModelException("Dimension mismatch. Matrix #" + Integer.toString(i - 1) + " for model " + name +
                                   " has " + Integer.toString(inputDim) + " rows, but matrix #" + Integer.toString(i) +
                                   " has " + Integer.toString(numCols - 1) + " non-bias columns.");
        }
      }
      
      if (i == weightMatrices.size() - 1 & numRows != 1) {
        throw new ModelException("Final matrix for model " + name + " has " + Integer.toString(numRows) +
                                 " rows, but should have 1 row.");
      }
      
      inputDim = numRows;
    }
  }

  @Override
  public float score(float[] inputFeatures) {

    float[] outputVec = inputFeatures;
    float[][] weightMatrix;
    int layers = weightMatrices.size();

    for (int layer = 0; layer < layers; layer++) {

      weightMatrix = weightMatrices.get(layer);
      outputVec = dot(weightMatrix, outputVec);

      if (layer < layers - 1) {
        for (int i = 0; i < outputVec.length; i++) {
          outputVec[i] = doNonlinearity(outputVec[i]);
        }
      }
    }

    return outputVec[0];
  }

  protected void validateNonlinearity(String key, String val) throws ModelException {
    if (!val.matches("relu|sigmoid|identity")) {
      throw new ModelException("Invalid "+key+" for model " + name + ". " +
                               "\"" + val + "\" is not \"relu\" or \"sigmoid\" or \"identity\".");
    }
  }

  protected void altValidate() throws ModelException {
    super.validate();

    if (inputFeatureNames.size() != inputFeatureIndices.length) {
      throw new ModelException("Model " + name
      + " inputFeatureNames.size()=" + Integer.toString(inputFeatureNames.size())
      + " inputFeatureIndices.length=" + Integer.toString(inputFeatureIndices.length));
    }

    for (int ii=0; ii<inputFeatureNames.size(); ++ii) {
      if (inputFeatureIndices[ii] < 0) {
        throw new ModelException("Model " + name + " input #" + Integer.toString(ii)
        + " ("+inputFeatureNames.get(ii)+") is not a model feature");
      }
    }

    int numOutputs = inputFeatureNames.size();

    int numLayers = 0;
    for (NeuralNetworkLayer layer : this.layers) {
      try {
        numOutputs = layer.validate(numOutputs);
      } catch (ModelException me) {
        throw new ModelException("Model " + name + " layer #" + Integer.toString(numLayers)
        + " validation failed.", me);
      }
      ++numLayers;
    }

    try {
      this.output.validate(numOutputs);
    } catch (ModelException me) {
      throw new ModelException("Model " + name + " output node validation failed.", me);
    }
  }

  public float altScore(float[] inputFeatures) {
    float[] outputVec = new float[inputFeatureIndices.length];
    for (int ii=0; ii<outputVec.length; ++ii) {
      outputVec[ii] = inputFeatures[inputFeatureIndices[ii]];
    }
    for (NeuralNetworkLayer layer : this.layers) {
      outputVec = layer.calculateOutputs(outputVec);
    }
    return output.calculateOutput(outputVec);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc,
                             float finalScore, List<Explanation> featureExplanations) {

    String modelDescription = "";

    for (int layer = 0; layer < weightMatrices.size(); layer++) {

      float[][] weightMatrix = weightMatrices.get(layer);
      int numCols = weightMatrix[0].length;

      if (layer == 0) {
        modelDescription += "Input has " + Integer.toString(numCols - 1) + " features.";
      } else {
        modelDescription += System.lineSeparator();
        modelDescription += "Hidden layer #" + Integer.toString(layer) + " has " + Integer.toString(numCols - 1);
        modelDescription += " fully connected units.";
      }
    }
    return Explanation.match(finalScore, modelDescription);
  }

}
