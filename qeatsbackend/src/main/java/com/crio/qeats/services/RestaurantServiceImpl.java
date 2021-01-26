
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import com.crio.qeats.utils.GeoUtils;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Log4j2
@Component
@Service
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  private final LocalTime morningPeakHourStartTime = LocalTime.parse("08:00:00");
  private final LocalTime morningPeakHourEndTime = LocalTime.parse("10:00:00");
  private final LocalTime afternoonPeakHourStartTime = LocalTime.parse("13:00:00");
  private final LocalTime afternoonPeakHourEndTime = LocalTime.parse("14:00:00");
  private final LocalTime eveningPeakHourStartTime = LocalTime.parse("19:00:00");
  private final LocalTime eveningPeakHourEndTime = LocalTime.parse("21:00:00");

  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;

  Boolean isWithinPeakHours(LocalTime currentTime) {
    if (currentTime.compareTo(morningPeakHourStartTime) >= 0
        && currentTime.compareTo(morningPeakHourEndTime) <= 0) {
      return true;
    } else if (currentTime.compareTo(afternoonPeakHourStartTime) >= 0
        && currentTime.compareTo(afternoonPeakHourEndTime) <= 0) {
      return true;
    } else if (currentTime.compareTo(eveningPeakHourStartTime) >= 0
        && currentTime.compareTo(eveningPeakHourEndTime) <= 0) {
      return true;
    }
    return false;
  }

  private Double getServingRadius(LocalTime currentTime) {
    Double servingRadiusInKms = 0.0;
    if (isWithinPeakHours(currentTime)) {
      servingRadiusInKms = peakHoursServingRadiusInKms;
    } else {
      servingRadiusInKms = normalHoursServingRadiusInKms;
    }
    return servingRadiusInKms;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    Double servingRadiusInKms = getServingRadius(currentTime);
    log.info("Coverage Radius: " + servingRadiusInKms.toString());
    List<Restaurant> restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        currentTime, servingRadiusInKms);

    return GetRestaurantsResponse.builder().restaurants(restaurants).build();
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search
  // string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
        GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    List<Restaurant> restaurants = new ArrayList<Restaurant>();

    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    String searchString = getRestaurantsRequest.getSearchFor();
    Double servingRadiusInKms = getServingRadius(currentTime);
    log.info("Coverage Radius: " + servingRadiusInKms.toString());

    if (!searchString.equals("")) {
      List<Restaurant> uniqueRestaurants = new ArrayList<Restaurant>();

      List<Restaurant> restaurantsByName = restaurantRepositoryService
          .findRestaurantsByName(latitude,
                longitude, searchString, currentTime, servingRadiusInKms);

      List<Restaurant> restaurantsByAttributes = restaurantRepositoryService
          .findRestaurantsByAttributes(latitude,
                longitude, searchString, currentTime, servingRadiusInKms);

      List<Restaurant> restaurantsByItemName = restaurantRepositoryService
          .findRestaurantsByItemName(latitude,
                longitude, searchString, currentTime, servingRadiusInKms);

      List<Restaurant> restaurantsByItemAttributes = restaurantRepositoryService
          .findRestaurantsByItemAttributes(latitude,
                longitude, searchString, currentTime, servingRadiusInKms);

      // add elements to set to ensure unique
      restaurantsByName.stream().forEach(restaurant -> {
        uniqueRestaurants.add(restaurant);
      });

      restaurantsByAttributes.stream().forEach(restaurant -> {
        uniqueRestaurants.add(restaurant);
      });

      restaurantsByItemName.stream().forEach(restaurant -> {
        uniqueRestaurants.add(restaurant);
      });

      restaurantsByItemAttributes.stream().forEach(restaurant -> {
        uniqueRestaurants.add(restaurant);
      });

      // add set's element to list
      uniqueRestaurants.stream().forEach(restaurant -> {
        restaurants.add(restaurant);
      });
    }

    return GetRestaurantsResponse.builder().restaurants(restaurants).build();
  }

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time
  // faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
        GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    int numThreads = 4;
    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    String searchString = getRestaurantsRequest.getSearchFor();
    Double servingRadiusInKms = getServingRadius(currentTime);
    List<Restaurant> restaurants = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<List<Restaurant>>> tasks = new ArrayList<Future<List<Restaurant>>>();

    Future<List<Restaurant>> future = executor.submit(new Callable<List<Restaurant>>() {
      @Override
      public List<Restaurant> call() throws Exception {
        return restaurantRepositoryService.findRestaurantsByName(latitude,
              longitude, searchString, currentTime,
            servingRadiusInKms);
      }
    });
    tasks.add(future);
    future = executor.submit(new Callable<List<Restaurant>>() {
      @Override
      public List<Restaurant> call() throws Exception {
        return restaurantRepositoryService.findRestaurantsByAttributes(latitude,
              longitude, searchString, currentTime, servingRadiusInKms);
      }
    });
    tasks.add(future);
    future = executor.submit(new Callable<List<Restaurant>>() {
      @Override
      public List<Restaurant> call() throws Exception {
        return  restaurantRepositoryService.findRestaurantsByItemName(latitude,
              longitude, searchString, currentTime, servingRadiusInKms);
      }
    });
    tasks.add(future);
    future = executor.submit(new Callable<List<Restaurant>>() {
      @Override
      public List<Restaurant> call() throws Exception {
        return restaurantRepositoryService
              .findRestaurantsByItemAttributes(latitude,
                    longitude, searchString, currentTime, servingRadiusInKms);
      }
    });
    tasks.add(future);

    tasks.stream().forEach(task -> {
      try {
        task.get().stream().forEach(t -> {
          restaurants.add(t);
        });
      } catch (RuntimeException e) {
        e.printStackTrace();
        log.info("aakash RuntimeException: {}", e.getMessage());
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        log.info("aakash InterruptedException: {}", e.getMessage());
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        log.info("aakash ExecutionException: {}", e.getMessage());
      } finally {
        executor.shutdown();
      }
    });

    return GetRestaurantsResponse.builder()
            .restaurants(restaurants)
            .build();
  }
}

