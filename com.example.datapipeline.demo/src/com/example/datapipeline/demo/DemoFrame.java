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

    private void buildUi() {
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

        JPanel stats = new JPanel(new GridLayout(2, 4, 12, 2));
        stats.setBorder(BorderFactory.createTitledBorder("Throughput"));
        stats.add(new JLabel("submitted", SwingConstants.CENTER));
        stats.add(new JLabel("processed", SwingConstants.CENTER));
        stats.add(new JLabel("dropped (overflow)", SwingConstants.CENTER));
        stats.add(new JLabel("UI updates", SwingConstants.CENTER));
        for (JLabel l : new JLabel[] { submittedLabel, processedLabel, droppedLabel, uiLabel }) {
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
    }
}
