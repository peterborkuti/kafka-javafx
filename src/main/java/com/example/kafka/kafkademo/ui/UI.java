package com.example.kafka.kafkademo.ui;

import com.example.kafka.kafkademo.ChartApplication;
import com.example.kafka.kafkademo.services.KafkaConsumer;
import com.example.kafka.kafkademo.services.KafkaProducer;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.geometry.Insets;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.Slider;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.kafka.receiver.ReceiverRecord;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Component
@Slf4j
public class UI implements ApplicationListener<ChartApplication.StageReadyEvent> {
	AtomicLong counts = new AtomicLong(0);
	BarChart<String, Number> barChart;
	final Label delayValue = new Label("");
	final Label sendedNumberValue = new Label("");
	int refreshValueInMillisecs = 5000;
	final Label refreshTimeValue = new Label("" + refreshValueInMillisecs);
	private KafkaProducer kafkaProducer;
	private KafkaConsumer kafkaConsumer;
	List<AtomicLong> datas =
			IntStream.range(0,256).mapToObj(i -> new AtomicLong(0)).collect(Collectors.toList());

	private Disposable consumerDisposable;
	public UI(KafkaConsumer kafkaConsumer, KafkaProducer kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
		this.kafkaConsumer = kafkaConsumer;
		delayValue.setText("" + kafkaProducer.getDelay());
		sendedNumberValue.setText("" + kafkaProducer.getSendingNumber());
		setRefresh(refreshValueInMillisecs);
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

	private void setRefresh(int refreshValue) {
		if (consumerDisposable != null && !consumerDisposable.isDisposed()) {
			consumerDisposable.dispose();
			try {
				while (!consumerDisposable.isDisposed()) {
					Thread.sleep(100);
				}
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
		}

		consumerDisposable = kafkaConsumer.getReceiver().map(ReceiverRecord::value).
				filter(this::isByteValue).buffer(Duration.ofMillis(refreshValue)).subscribe(this::consumeRecord);
		refreshValueInMillisecs = refreshValue;
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
		bc.setAnimated(false);

		setData();

		return bc;
	}
	private Slider getDelaySlider() {
		return getSlider(1, 1000, 100, 10,
				kafkaProducer.getDelay(), kafkaProducer::setDelay, delayValue);
	}

	private Slider getDataSlider() {
		return getSlider(-128, 127, 10, 10,
				kafkaProducer.getSendingNumber(), kafkaProducer::setSendingNumber, sendedNumberValue);
	}


	private Slider getRefreshTime() {
		return getSlider(100, 5000, 1000, 100,
				refreshValueInMillisecs, this::setRefresh, refreshTimeValue);
	}

	private Slider getSlider(
			int min, int max, int majorThickUnit, int blockIncrement, int initialValue,
			Consumer<Integer> valueSetter, Label labelToSet) {
		Slider slider = new Slider(min, max, initialValue);
		slider.setShowTickMarks(true);
		slider.setShowTickLabels(true);
		slider.setMajorTickUnit(majorThickUnit);
		slider.setBlockIncrement(blockIncrement);

		slider.valueProperty().addListener(new ChangeListener<Number>() {
			public void changed(ObservableValue<? extends Number> ov,
								Number old_val, Number new_val) {
				valueSetter.accept(new_val.intValue());
				labelToSet.setText("" + new_val.intValue());
			}
		});

		return slider;
	}

	private void addSlider(GridPane grid, String labelText, Label valueLabel, int row, Slider slider) {
		Label label = new Label(labelText);
		GridPane.setConstraints(label, 0, row, 2, 1);
		grid.getChildren().add(label);

		GridPane.setConstraints(valueLabel, 2, row);
		grid.getChildren().add(valueLabel);

		GridPane.setConstraints(slider, 0, row+1, 3, 1);
		grid.getChildren().add(slider);
	}

	private void draw(Stage stage) {
		Group root = new Group();
		Scene scene = new Scene(root, 600, 400);
		stage.setScene(scene);
		stage.setTitle("Kafka demo");
		scene.setFill(Color.BLACK);

		GridPane grid = new GridPane();
		grid.setPadding(new Insets(10, 10, 10, 10));
		grid.setVgap(10);
		grid.setHgap(70);

		barChart = getBarChart();

		/* Chart Row 0, col 0-2 */
		GridPane.setConstraints(barChart, 0, 0);
		GridPane.setColumnSpan(barChart, 3);
		grid.getChildren().add(barChart);

		scene.setRoot(grid);

		addSlider(grid, "Producer delay in ms", delayValue, 1, getDelaySlider());
		addSlider(grid, "Producer data", sendedNumberValue, 3, getDataSlider());
		addSlider(grid, "Refresh time in millis", refreshTimeValue, 5, getRefreshTime());

		stage.show();
	}

	@Override
	public void onApplicationEvent(ChartApplication.StageReadyEvent stageReadyEvent) {
		draw(stageReadyEvent.getStage());
	}
}
