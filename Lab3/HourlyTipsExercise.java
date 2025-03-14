package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */

public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// Чтение параметров
		ParameterTool params = ParameterTool.fromArgs(args);
		final String inputPath = params.get("input", ExerciseBase.pathToFareData);

		final int maxDelay = 60;       // Максимальная задержка событий - 60 секунд
		final int speedFactor = 600;   // События за 10 минут подаются за 1 секунду

		// Настройка среды выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// Источник данных с чаевыми
		DataStream<TaxiFare> fareStream = env.addSource(
				fareSourceOrTest(new TaxiFareSource(inputPath, maxDelay, speedFactor)));

		// Вычисление максимальных чаевых за каждый час
		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fareStream
				.keyBy(fare -> fare.driverId)                       // Группировка по идентификатору водителя
				.timeWindow(Time.hours(1))                          // Оконное агрегирование по часу
				.process(new CalculateTips())                       // Подсчет суммы чаевых
				.timeWindowAll(Time.hours(1))                       // Сборка по всем водителям за каждый час
				.maxBy(2);                                          // Нахождение максимальных чаевых

		// Вывод результата или тестирование
		printOrTest(hourlyMaxTips);

		// Запуск выполнения пайплайна
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Вспомогательная функция для подсчета суммы чаевых в окне.
	 */
	public static class CalculateTips extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long driverId, Context context, Iterable<TaxiFare> fares,
							Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			float totalTips = 0F;
			// Суммирование чаевых
			for (TaxiFare fare : fares) {
				totalTips += fare.tip;
			}
			// Вывод результата: конец окна, ID водителя, сумма чаевых
			out.collect(new Tuple3<>(context.window().getEnd(), driverId, totalTips));
		}
	}
}