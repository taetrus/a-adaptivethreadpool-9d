package com.example.datapipeline.demo;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.concurrent.atomic.AtomicLong;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.Timer;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.ExecutionMode;
import com.example.datapipeline.api.OverflowPolicy;
import com.example.datapipeline.api.PipelineFactory;
import com.example.datapipeline.api.UiUpdateMode;

/** Interactive dashboard: fake source → DataPipeline → live stats. */
final class DemoFrame extends JFrame {

    private final PipelineFactory factory;
    private final Runnable onClose;

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong uiUpdates = new AtomicLong();
    private long lastSubmitted, lastProcessed, lastDropped, lastUi;

    private volatile int processingDelayMs = 5;
    private volatile DataPipeline<Tick, String> pipeline;
    private final TickSource source;
    private boolean sourceRunning;

    private final JComboBox<OverflowPolicy> policyCombo =
            new JComboBox<>(OverflowPolicy.values());
    private final JComboBox<String> execCombo =
            new JComboBox<>(new String[] { "SEQUENTIAL", "PARALLEL_ORDERED" });
    private final JSpinner threadsSpinner = new JSpinner(new SpinnerNumberModel(4, 2, 8, 1));
    private final JComboBox<String> uiModeCombo =
            new JComboBox<>(new String[] { "IMMEDIATE", "PERIODIC" });
    private final JSpinner periodSpinner = new JSpinner(new SpinnerNumberModel(100, 16, 1000, 16));
    private final JCheckBox tickPullCheck = new JCheckBox("process only on tick");
    private final JSlider rateSlider = new JSlider(100, 10_000, 2_000);
    private final JSlider delaySlider = new JSlider(0, 50, 5);
    private final JButton startStop = new JButton("Start");

    private final JLabel liveValue = new JLabel("—", SwingConstants.CENTER);
    private final JLabel submittedLabel = new JLabel();
    private final JLabel processedLabel = new JLabel();
    private final JLabel droppedLabel = new JLabel();
    private final JLabel uiLabel = new JLabel();
    private final JLabel threadsLabel = new JLabel();
    private final Timer statsTimer;

    DemoFrame(PipelineFactory factory, Runnable onClose) {
        super("DataPipeline Demo");
        this.factory = factory;
        this.onClose = onClose;
        this.source = new TickSource(this::submitToPipeline, submitted, rateSlider.getValue());

        buildUi();
        wireListeners();
        rebuildPipeline();

        statsTimer = new Timer(1000, e -> refreshStats());
        statsTimer.start();

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                onClose.run();
            }
        });
        pack();
        setLocationByPlatform(true);

        if (Boolean.getBoolean("demo.autostart")) {
            startStop.doClick();
        }
    }

    private void submitToPipeline(Tick t) {
        DataPipeline<Tick, String> p = pipeline;
        if (p != null) p.submit(t);
    }

    // ---- pipeline lifecycle ----

    private void rebuildPipeline() {
        DataPipeline<Tick, String> old = pipeline;
        pipeline = null;                 // source keeps running; ticks go nowhere during swap
        if (old != null) old.close();

        OverflowPolicy policy = (OverflowPolicy) policyCombo.getSelectedItem();
        boolean parallel = "PARALLEL_ORDERED".equals(execCombo.getSelectedItem());
        boolean periodic = "PERIODIC".equals(uiModeCombo.getSelectedItem());

        DataPipeline.Builder<Tick, String> b = DataPipeline.<Tick, String>builder()
                .processor(this::process)
                .uiConsumer(this::showResult)
                .overflowPolicy(policy)
                .executionMode(parallel
                        ? ExecutionMode.parallelOrdered((Integer) threadsSpinner.getValue())
                        : ExecutionMode.SEQUENTIAL)
                .uiUpdateMode(periodic
                        ? UiUpdateMode.periodic((Integer) periodSpinner.getValue())
                        : UiUpdateMode.immediate())
                .processOnlyOnTick(tickPullCheck.isSelected())
                .onError((t, item) -> System.err.println("[demo] pipeline error on " + item + ": " + t));
        if (policy == OverflowPolicy.CONFLATE) {
            b.conflator(Tick::conflate);
        }
        if (policy == OverflowPolicy.PROCESS_ALL) {
            b.onOverflow(t -> dropped.incrementAndGet());
        }
        pipeline = factory.build(b);
    }

    private String process(Tick t) {
        int d = processingDelayMs;
        if (d > 0) {
            try {
                Thread.sleep(d);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        processed.incrementAndGet();
        return t.merged > 1 ? "#" + t.id + "  (merged " + t.merged + " ticks)" : "#" + t.id;
    }

    private void showResult(String s) {
        uiUpdates.incrementAndGet();
        liveValue.setText(s);
    }

    void shutdown() {
        statsTimer.stop();
        source.stop();
        DataPipeline<Tick, String> p = pipeline;
        pipeline = null;
        if (p != null) p.close();
    }

    // ---- UI plumbing ----

    private void wireListeners() {
        Runnable rebuild = this::rebuildPipeline;
        policyCombo.addActionListener(e -> rebuild.run());
        execCombo.addActionListener(e -> {
            threadsSpinner.setEnabled("PARALLEL_ORDERED".equals(execCombo.getSelectedItem()));
            rebuild.run();
        });
        threadsSpinner.addChangeListener(e -> rebuild.run());
        uiModeCombo.addActionListener(e -> {
            boolean periodic = "PERIODIC".equals(uiModeCombo.getSelectedItem());
            periodSpinner.setEnabled(periodic);
            tickPullCheck.setEnabled(periodic);
            if (!periodic) tickPullCheck.setSelected(false);
            rebuild.run();
        });
        periodSpinner.addChangeListener(e -> rebuild.run());
        tickPullCheck.addActionListener(e -> rebuild.run());
        rateSlider.addChangeListener(e -> source.setRate(rateSlider.getValue()));
        delaySlider.addChangeListener(e -> processingDelayMs = delaySlider.getValue());
        startStop.addActionListener(e -> {
            if (sourceRunning) {
                source.stop();
                startStop.setText("Start");
            } else {
                source.start();
                startStop.setText("Stop");
            }
            sourceRunning = !sourceRunning;
        });
        threadsSpinner.setEnabled(false);
        periodSpinner.setEnabled(false);
        tickPullCheck.setEnabled(false);
    }

    private void installTooltips() {
        policyCombo.setToolTipText("<html>What happens to backlog when data arrives faster than it is processed:<br>"
                + "<b>PROCESS_ALL</b> — bounded buffer, every item processed, oldest dropped when full<br>"
                + "<b>LATEST_WINS</b> — single slot, new data overwrites unprocessed data<br>"
                + "<b>CONFLATE</b> — pending ticks merged into one (watch for \"merged N ticks\")</html>");
        execCombo.setToolTipText("<html><b>SEQUENTIAL</b> — one worker thread, in order<br>"
                + "<b>PARALLEL_ORDERED</b> — a thread pool processes concurrently; results are<br>"
                + "re-sequenced so the UI still sees them in submission order.<br>"
                + "Only useful with PROCESS_ALL — other policies degrade to sequential (see console warning).</html>");
        threadsSpinner.setToolTipText("Pool size for PARALLEL_ORDERED. Higher = more throughput while the processor is the bottleneck.");
        uiModeCombo.setToolTipText("<html><b>IMMEDIATE</b> — every result goes to the EDT as it completes<br>"
                + "(coalesced: if the EDT is behind, only the newest pending result is shown)<br>"
                + "<b>PERIODIC</b> — a scheduler pushes the newest result every N ms; nothing between ticks</html>");
        periodSpinner.setToolTipText("UI refresh period in milliseconds for PERIODIC mode.");
        tickPullCheck.setToolTipText("<html>Inverts the flow: nothing is processed between UI ticks.<br>"
                + "Each tick pulls the freshest data from intake, processes it once, then paints.<br>"
                + "With LATEST_WINS: exactly one computation per UI frame. Requires PERIODIC mode.</html>");
        rateSlider.setToolTipText("How many ticks per second the fake source emits (100–10,000). Applies live.");
        delaySlider.setToolTipText("Artificial cost of processing one tick, in ms. At 5 ms one worker caps at ~200/s — raise the rate above that to see the overflow policy engage. Applies live.");
        startStop.setToolTipText("Start or stop the fake data source. The pipeline itself stays up.");
        liveValue.setToolTipText("The most recent result delivered to the Swing EDT. \"merged N ticks\" appears under CONFLATE.");
        submittedLabel.setToolTipText("Ticks the source pushed into pipeline.submit() — per second (total).");
        processedLabel.setToolTipText("Ticks the processor actually ran — per second (total). Under LATEST_WINS/CONFLATE this is far below submitted: stale ticks were skipped or merged, not processed.");
        droppedLabel.setToolTipText("PROCESS_ALL only: oldest items dropped by the bounded buffer, reported via onOverflow. Always 0 for other policies — they discard by overwriting, not by overflow.");
        uiLabel.setToolTipText("Results delivered to the EDT — per second (total). Below processed when coalescing or PERIODIC mode skips intermediate results.");
        threadsLabel.setToolTipText("<html>Live JVM threads right now — total (of which <b>datapipeline-*</b>).<br>"
                + "The pipeline's count is bounded by construction and set at build time:<br>"
                + "it never grows with load. Change the config and watch it move.</html>");
    }

    private void buildUi() {
        installTooltips();
        JPanel config = new JPanel(new GridBagLayout());
        config.setBorder(BorderFactory.createTitledBorder("Pipeline configuration"));
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 6, 2, 6);
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.HORIZONTAL;
        int row = 0;
        addRow(config, c, row++, "Overflow policy", policyCombo);
        addRow(config, c, row++, "Execution mode", execCombo);
        addRow(config, c, row++, "Worker threads", threadsSpinner);
        addRow(config, c, row++, "UI update mode", uiModeCombo);
        addRow(config, c, row++, "Period (ms)", periodSpinner);
        addRow(config, c, row++, "", tickPullCheck);
        addRow(config, c, row++, "Emit rate (ticks/s)", rateSlider);
        addRow(config, c, row++, "Processing delay (ms)", delaySlider);
        addRow(config, c, row, "", startStop);

        liveValue.setFont(liveValue.getFont().deriveFont(Font.BOLD, 32f));
        liveValue.setBorder(BorderFactory.createTitledBorder("Latest result"));

        JPanel stats = new JPanel(new GridLayout(2, 5, 12, 2));
        stats.setBorder(BorderFactory.createTitledBorder("Throughput"));
        String[] headers = { "submitted", "processed", "dropped (overflow)", "UI updates", "threads" };
        JLabel[] values = { submittedLabel, processedLabel, droppedLabel, uiLabel, threadsLabel };
        for (int i = 0; i < headers.length; i++) {
            JLabel h = new JLabel(headers[i], SwingConstants.CENTER);
            h.setToolTipText(values[i].getToolTipText());
            stats.add(h);
        }
        for (JLabel l : values) {
            l.setHorizontalAlignment(SwingConstants.CENTER);
            l.setFont(l.getFont().deriveFont(Font.BOLD));
            stats.add(l);
        }
        refreshStats();

        JPanel root = new JPanel(new BorderLayout(8, 8));
        root.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        root.add(config, BorderLayout.NORTH);
        root.add(liveValue, BorderLayout.CENTER);
        root.add(stats, BorderLayout.SOUTH);
        setContentPane(root);
    }

    private static void addRow(JPanel p, GridBagConstraints c, int row, String label, java.awt.Component comp) {
        c.gridx = 0; c.gridy = row; c.weightx = 0;
        p.add(new JLabel(label), c);
        c.gridx = 1; c.weightx = 1;
        p.add(comp, c);
    }

    private void refreshStats() {
        long s = submitted.get(), pr = processed.get(), d = dropped.get(), u = uiUpdates.get();
        submittedLabel.setText((s - lastSubmitted) + "/s  (" + s + ")");
        processedLabel.setText((pr - lastProcessed) + "/s  (" + pr + ")");
        droppedLabel.setText((d - lastDropped) + "/s  (" + d + ")");
        uiLabel.setText((u - lastUi) + "/s  (" + u + ")");
        lastSubmitted = s; lastProcessed = pr; lastDropped = d; lastUi = u;
        threadsLabel.setText(countThreads());
    }

    private static String countThreads() {
        Thread[] threads = new Thread[Thread.activeCount() * 2 + 8];
        int n = Thread.enumerate(threads);
        int pipeline = 0;
        for (int i = 0; i < n; i++) {
            if (threads[i].getName().startsWith("datapipeline-")) pipeline++;
        }
        return n + "  (" + pipeline + " pipeline)";
    }
}
