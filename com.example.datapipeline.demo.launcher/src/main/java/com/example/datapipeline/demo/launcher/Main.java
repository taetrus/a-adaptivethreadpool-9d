package com.example.datapipeline.demo.launcher;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.osgi.framework.wiring.BundleRevision;

/**
 * Boots an embedded Felix framework, installs every jar found in the
 * {@code bundles/} directory next to this launcher jar (override with
 * {@code -Ddemo.bundles.dir=...}), starts them, and waits until the demo
 * window's close stops the framework.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        File bundlesDir = resolveBundlesDir();
        File[] jars = bundlesDir.listFiles((d, n) -> n.endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            System.err.println("No bundles found in " + bundlesDir.getAbsolutePath());
            System.exit(1);
        }

        Map<String, String> config = new HashMap<>();
        config.put(Constants.FRAMEWORK_STORAGE,
                Files.createTempDirectory("datapipeline-demo-felix").toString());
        config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);

        FrameworkFactory factory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        Framework framework = factory.newFramework(config);
        framework.init();
        BundleContext ctx = framework.getBundleContext();

        List<Bundle> installed = new ArrayList<>();
        for (File jar : jars) {
            installed.add(ctx.installBundle(jar.toURI().toString()));
        }
        framework.start();
        for (Bundle b : installed) {
            if ((b.adapt(BundleRevision.class).getTypes() & BundleRevision.TYPE_FRAGMENT) == 0) {
                b.start();
            }
        }
        System.out.println("[demo] framework started, " + installed.size()
                + " bundles installed; close the window to exit.");
        framework.waitForStop(0);
        System.exit(0);
    }

    private static File resolveBundlesDir() throws Exception {
        String prop = System.getProperty("demo.bundles.dir");
        if (prop != null) return new File(prop);
        File self = new File(Main.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        return new File(self.getParentFile(), "bundles");
    }
}
