
package at.jku.dke.kgolap.demo.datasets;

public class DemoDataset4DAbstractedLargeContextSmallFact extends DemoDataset4DAbstractedLargeContext {
  public DemoDataset4DAbstractedLargeContextSmallFact() {
    super();
    
    this.setNoOfFillerIterations(8);
    this.setNoOfAirports(10);
    this.setNoOfRunways(20);
    this.setNoOfTaxiways(60);
    this.setNoOfVors(10);
  }
}
