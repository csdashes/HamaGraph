/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class Stages {
    private int stage, 
            maxStage;

    Stages(int maxStage) {
        this.stage = 0;
        this.maxStage = maxStage;
    }
    
    /**
     * @return the stage
     */
    public int getStage() {
        return stage;
    }

    public void increaseStage() {
        this.stage = (this.stage + 1)%this.maxStage;
    }
    
}
