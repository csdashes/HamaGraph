/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class Step {
    private int step, 
            maxStep;

    Step(int maxStep) {
        this.step = 0;
        this.maxStep = maxStep;
    }
    
    /**
     * @return the step
     */
    public int getStep() {
        return step;
    }

    public void increaseStep() {
        this.step = (this.step + 1)%this.maxStep;
    }
    
}
