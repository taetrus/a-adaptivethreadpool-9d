package com.example.datapipeline.demo;

import javax.swing.SwingUtilities;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import com.example.datapipeline.api.PipelineFactory;

/**
 * DS component (declared in OSGI-INF/component.xml). Opens the demo window on
 * activation; closing the window stops the OSGi framework, which deactivates
 * this component and lets PipelineFactory close its pipelines.
 */
public final class DemoComponent {

    private volatile PipelineFactory factory;
    private volatile DemoFrame frame;

    public void setPipelineFactory(PipelineFactory f) {
        this.factory = f;
    }

    public void activate(BundleContext context) {
        final Bundle system = context.getBundle(0);
        SwingUtilities.invokeLater(() -> {
            DemoFrame f = new DemoFrame(factory, () -> stopFramework(system));
            frame = f;
            f.setVisible(true);
        });
    }

    public void deactivate() {
        SwingUtilities.invokeLater(() -> {
            DemoFrame f = frame;
            if (f != null) {
                frame = null;
                f.shutdown();
                f.dispose();
            }
        });
    }

    private static void stopFramework(Bundle system) {
        // Off the EDT: framework stop waits for bundle deactivation.
        Thread t = new Thread(() -> {
            try {
                system.stop();
            } catch (BundleException ignored) {
            }
        }, "demo-shutdown");
        t.setDaemon(true);
        t.start();
    }
}
