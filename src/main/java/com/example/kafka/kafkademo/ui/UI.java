package com.example.kafka.kafkademo.ui;

import com.example.kafka.kafkademo.ChartApplication;
import com.example.kafka.kafkademo.Consumer;
import com.example.kafka.kafkademo.Producer;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Slider;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.kafka.receiver.ReceiverRecord;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Component
@Slf4j
public class UI implements ApplicationListener<ChartApplication.StageReadyEvent> {
	AtomicLong counts = new AtomicLong(0);
	BarChart<String, Number> barChart;
	private Producer producer;
	List<AtomicLong> datas =
			IntStream.range(0,256).mapToObj(i -> new AtomicLong(0)).collect(Collectors.toList());

	//private SimpleHistogramDataset dataset;
	private Disposable consumerDisposable;
	public UI(Consumer consumer, Producer producer) {
		//dataset = new SimpleHistogramDataset("Key");
		//IntStream.range(-128, 128).forEach(i -> dataset.addBin(new SimpleHistogramBin(i, i+1, true, false)));
		consumerDisposable = consumer.getReceiver().map(ReceiverRecord::value).filter(this::isByteValue).buffer(Duration.ofSeconds(5)).subscribe(this::consumeRecord);
		this.producer = producer;
	}

	private boolean isByteValue(String value) {
		try {
			Byte.valueOf(value);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
	}
	@PreDestroy
	private void unsubscribe() {
		consumerDisposable.dispose();
	}

	private void consumeRecord(List<String> stringValues) {
		try {
			stringValues.stream().mapToInt(Integer::valueOf).map(i -> i + 128)
					.forEach(i -> datas.get(i).getAndIncrement());
			//log.error(datas.stream().map(a -> "" + a.get()).collect(Collectors.joining(",")));
			log.error("Got " + stringValues.size() + " values");
			counts.getAndAdd(stringValues.size());
			Platform.runLater(() -> setData());
		} catch (Exception e) {
			log.error("Can not display values", e);
		}
	}

	/*
	private JFreeChart createChart() {
		JFreeChart chart = ChartFactory.createHistogram("Histogram",
				"Numbers between -128 and 127", "Counts", dataset, PlotOrientation.VERTICAL,
				false, false, false);
		XYPlot plot = (XYPlot) chart.getPlot();
		plot.setDomainZeroBaselineVisible(false);
		plot.getDomainAxis().setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		plot.getRangeAxis().setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		return chart;
	}

	 */

	private void setData() {
		if (barChart == null) {
			return;
		}
		XYChart.Series series = new XYChart.Series();
		series.setName("Counts");
		IntStream.range(-128, 128).forEach(i -> series.getData().add(new XYChart.Data(""+i, datas.get(i + 128))));
		synchronized (barChart) {
			barChart.getData().clear();
			barChart.getData().add(series);
			barChart.getYAxis().setLabel(String.format("Counts(%d)", counts.get()));
		}
	}

	private BarChart<String, Number> getBarChart() {
		final CategoryAxis xAxis = new CategoryAxis();
		final NumberAxis yAxis = new NumberAxis();
		BarChart<String, Number> bc = new BarChart<>(xAxis,yAxis);
		bc.setTitle("Histogram");
		xAxis.setLabel("Numbers between -128 and 127");

		setData();

		return bc;
	}

	private Slider getSlider() {
		Slider slider = new Slider(0, 1000, producer.getDelay());
		slider.setShowTickMarks(true);
		slider.setShowTickLabels(true);
		slider.setMajorTickUnit(100);
		slider.setBlockIncrement(10);

		slider.valueProperty().addListener(new ChangeListener<Number>() {
			public void changed(ObservableValue<? extends Number> ov,
								Number old_val, Number new_val) {
				producer.setDelay(new_val.intValue());
			}
		});

		return slider;
	}

	private void draw(Stage stage) {
		GridPane p = new GridPane();
		barChart = getBarChart();
		p.add(barChart, 0, 0);
		p.add(getSlider(), 0, 1);
		stage.setScene(new Scene(p));
		stage.setTitle("FXChart");
		stage.setWidth(640);
		stage.setHeight(480);
		stage.show();
	}

	@Override
	public void onApplicationEvent(ChartApplication.StageReadyEvent stageReadyEvent) {
		draw(stageReadyEvent.getStage());
	}
}
