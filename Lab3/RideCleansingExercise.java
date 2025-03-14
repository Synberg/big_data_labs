/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */

public class RideCleansingExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		// Получаем параметры запуска
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // Максимальная задержка событий — 60 секунд
		final int servingSpeedFactor = 600; // События 10 минут обрабатываются за 1 секунду

		// Создаем среду выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism); // Устанавливаем параллелизм

		// Загружаем поток данных о поездках
		DataStream<TaxiRide> rides = env.addSource(
				rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// Фильтрация поездок, которые начинаются и заканчиваются в Нью-Йорке
		DataStream<TaxiRide> filteredRides = rides.filter(new NYCFilter());

		// Выводим отфильтрованные поездки
		printOrTest(filteredRides);

		// Запускаем выполнение задачи
		env.execute("Taxi Ride Cleansing");
	}

	/**
	 * Класс фильтрации поездок, оставляющий только те, которые начинаются и заканчиваются в Нью-Йорке.
	 */
	private static class NYCFilter implements FilterFunction<TaxiRide> {

		// Границы Нью-Йорка по широте и долготе
		private static final float MIN_LAT = 40.4774f; // Минимальная широта
		private static final float MAX_LAT = 40.9176f; // Максимальная широта
		private static final float MIN_LON = -74.2591f; // Минимальная долгота
		private static final float MAX_LON = -73.7004f; // Максимальная долгота

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			// Проверка, находится ли начало и конец поездки в пределах Нью-Йорка
			boolean isStartInNYC = isWithinNYC(taxiRide.startLat, taxiRide.startLon);
			boolean isEndInNYC = isWithinNYC(taxiRide.endLat, taxiRide.endLon);

			// Оставляем только те поездки, которые начинаются и заканчиваются в NYC
			return isStartInNYC && isEndInNYC;
		}

		/**
		 * Проверка, находятся ли координаты в пределах границ Нью-Йорка.
		 * @param lat широта
		 * @param lon долгота
		 * @return true, если координаты находятся в пределах NYC
		 */
		private boolean isWithinNYC(float lat, float lon) {
			return lat >= MIN_LAT && lat <= MAX_LAT && lon >= MIN_LON && lon <= MAX_LON;
		}
	}
}
