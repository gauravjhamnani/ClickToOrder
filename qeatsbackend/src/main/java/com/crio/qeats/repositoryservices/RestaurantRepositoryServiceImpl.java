/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;

import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Provider;

import lombok.extern.log4j.Log4j2;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;

@Log4j2
@Service
@Primary
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  @Autowired
  private RestaurantRepository restaurantRepository;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  private List<RestaurantEntity> getAllrestaurantsFromDb(Double latitude,
      Double longitude, Double servingRadiusInKms) {
   
    return restaurantRepository.findAll();
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

    // CHECKSTYLE:OFF
    
    // TODO: CRIO_TASK_MODULE_REDIS
    // We want to use cache to speed things up. Write methods that perform the same functionality,
    // but using the cache if it is present and reachable.
    // Remember, you must ensure that if cache is not present, the queries are directed at the
    // database instead.

    List<RestaurantEntity> allRestaurants = new ArrayList<RestaurantEntity>();
    Jedis jedis = null;
    try {
      jedis = redisConfiguration.getJedisPool().getResource();
      GeoHash geoHash = GeoHash.withCharacterPrecision(latitude, longitude, 7);
      log.info("aakash jedis.get(geoHash.toBase32() hai = {}", jedis.get(geoHash.toBase32()));
      if (jedis.get(geoHash.toBase32()) != null) {
        String cachedResponse = jedis.get(geoHash.toBase32());
        log.info("this is my cached response: {}", cachedResponse);
        try {
          allRestaurants = Arrays.asList(new ObjectMapper().readValue(cachedResponse, RestaurantEntity[].class));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          log.info("aakashIOException: " + e.getMessage());
          allRestaurants = getAllrestaurantsFromDb(latitude, longitude, 5.0);
          try {
            jedis.setex(geoHash.toBase32(),
            RedisConfiguration.REDIS_ENTRY_EXPIRY_IN_SECONDS,
            new ObjectMapper().writeValueAsString(allRestaurants));
          } catch (JsonProcessingException jsonParsingException) {
            // TODO Auto-generated catch block
            jsonParsingException.printStackTrace();
            log.info("aakashJsonProcessingException: " + jsonParsingException.getMessage());
          }
        }
      } else {
        allRestaurants = getAllrestaurantsFromDb(latitude, longitude, 5.0);
        try {
          jedis.setex(geoHash.toBase32(),
          RedisConfiguration.REDIS_ENTRY_EXPIRY_IN_SECONDS,
          new ObjectMapper().writeValueAsString(allRestaurants));
        } catch (JsonProcessingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          log.info("aakashJsonProcessingException: " + e.getMessage());
        }
      }
      // //CHECKSTYLE:ON
    } catch (RuntimeException e) {
      e.printStackTrace();
      allRestaurants = getAllrestaurantsFromDb(latitude, longitude, 5.0);
      // throw new RuntimeException("aakash" + e.getMessage());
    } finally {
      if (jedis != null) {
        redisConfiguration.getJedisPool().returnResource(jedis);
        jedis = null;
      }
    }
    log.info("allrestaurants size: {}", allRestaurants.size());
    List<Restaurant> restaurants = allRestaurants.stream().filter(restaurant -> {
      return isRestaurantCloseByAndOpen(restaurant,
          currentTime, latitude, longitude, servingRadiusInKms);
    }).map(restaurant -> {
      return modelMapperProvider.get().map(restaurant, Restaurant.class);
    }).collect(Collectors.toList());
    // //CHECKSTYLE:ON
    return restaurants;

  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objective:
  // 1. Check if a restaurant is nearby and open. If so, it is a candidate to be
  // returned.
  // NOTE: How far exactly is "nearby"?

  /**
   * Utility method to check if a restaurant is within the serving radius at a
   * given time.
   * 
   * @return boolean True if restaurant falls within serving radius and is open,
   *         false otherwise
  */

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    // see how to use mongo like query parameter (aakash)
    log.info("Reaching checkpoint 3");
    Optional<List<RestaurantEntity>> allRestaurantsByNameExactOptional = restaurantRepository
        .findRestaurantsByNameExact(searchString);
    Optional<List<RestaurantEntity>> allRestaurantsByNameContainingOptional = restaurantRepository
        .findRestaurantsByNameContaining(searchString);
    
    List<RestaurantEntity> allRestaurantsByNameExact = new ArrayList<>();
    List<RestaurantEntity> allRestaurantsByNameContaining = new ArrayList<>();

    if (allRestaurantsByNameExactOptional.isPresent()) {
      allRestaurantsByNameExact = allRestaurantsByNameExactOptional.get();
    }

    if (allRestaurantsByNameContainingOptional.isPresent()) {
      allRestaurantsByNameContaining = allRestaurantsByNameContainingOptional.get();
    }

    log.info("allRestauransByNameExact :- {}", allRestaurantsByNameExact);
    log.info("allRestauransByNameContaining :- {}", allRestaurantsByNameContaining);
    List<Restaurant> byRestaurantNameRestaurants = new ArrayList<Restaurant>(); 
    allRestaurantsByNameExact.stream()
        .forEach(restaurantEntity -> {
          if (isRestaurantCloseByAndOpen(restaurantEntity,
              currentTime, latitude, longitude, servingRadiusInKms)) {
            Restaurant restaurant = modelMapperProvider
                .get().map(restaurantEntity, Restaurant.class);
            if (!byRestaurantNameRestaurants.contains(restaurant)) {
              byRestaurantNameRestaurants.add(
                  modelMapperProvider.get().map(restaurantEntity, Restaurant.class)
              );
            }
          }
        });

    allRestaurantsByNameContaining.stream()
        .forEach(restaurantEntity -> {
          if (isRestaurantCloseByAndOpen(restaurantEntity,
              currentTime, latitude, longitude, servingRadiusInKms)) {
            Restaurant restaurant = modelMapperProvider
                .get().map(restaurantEntity, Restaurant.class);
            if (!byRestaurantNameRestaurants.contains(restaurant)) {
              byRestaurantNameRestaurants.add(
                  modelMapperProvider.get().map(restaurantEntity, Restaurant.class)
              );
            }
          }
        });
    log.info("byRestaurantNameRestaurants :- {}", byRestaurantNameRestaurants);
    return byRestaurantNameRestaurants;
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    Optional<List<RestaurantEntity>> allRestaurantsByAttributesExactOptional =
        restaurantRepository.findRestaurantsByAttributesExact(searchString);

    Optional<List<RestaurantEntity>> allRestaurantsByAttributesContainingOptional =
        restaurantRepository.findRestaurantsByAttributesContaining(searchString);

    List<RestaurantEntity> allRestaurantsByAttributesExact = new ArrayList<>();
    List<RestaurantEntity> allRestaurantsByAttributesContaining = new ArrayList<>();
    List<Restaurant> byAttributesRestaurants = new ArrayList<>();

    if (allRestaurantsByAttributesExactOptional.isPresent()) {
      allRestaurantsByAttributesExact = allRestaurantsByAttributesExactOptional.get();
    }

    if (allRestaurantsByAttributesContainingOptional.isPresent()) {
      allRestaurantsByAttributesContaining = allRestaurantsByAttributesContainingOptional.get();
    }



    allRestaurantsByAttributesExact.stream()
        .forEach(restaurantEntity -> {
          if (isRestaurantCloseByAndOpen(restaurantEntity,
              currentTime, latitude, longitude, servingRadiusInKms)) {
            Restaurant restaurant = modelMapperProvider
                .get().map(restaurantEntity, Restaurant.class);
            if (!byAttributesRestaurants.contains(restaurant)) {
              byAttributesRestaurants.add(
                  modelMapperProvider.get().map(restaurantEntity, Restaurant.class)
              );
            }
          }
        });

    allRestaurantsByAttributesContaining.stream()
        .forEach(restaurantEntity -> {
          if (isRestaurantCloseByAndOpen(restaurantEntity,
              currentTime, latitude, longitude, servingRadiusInKms)) {
            Restaurant restaurant = modelMapperProvider
                .get().map(restaurantEntity, Restaurant.class);
            if (!byAttributesRestaurants.contains(restaurant)) {
              byAttributesRestaurants.add(
                  modelMapperProvider.get().map(restaurantEntity, Restaurant.class)
              );
            }
          }
        });
    
    return byAttributesRestaurants;
  }



  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    Optional<List<RestaurantEntity>> allRestaurantsByItemNameOptional = restaurantRepository
        .findRestaurantsByNameExact(searchString);

    List<RestaurantEntity> allRestaurantsByItemName = new ArrayList<>();

    if (allRestaurantsByItemNameOptional.isPresent()) {
      allRestaurantsByItemName = allRestaurantsByItemNameOptional.get();
    }

    List<Restaurant> byItemNameRestaurants = allRestaurantsByItemName.stream()
        .filter(restaurant -> {
          return isRestaurantCloseByAndOpen(restaurant,
              currentTime, latitude, longitude, servingRadiusInKms);
        }).map(restaurant -> {
          return modelMapperProvider.get().map(restaurant, Restaurant.class);
        }).collect(Collectors.toList());
    
    return byItemNameRestaurants;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    // List<RestaurantEntity> allRestaurantsByItemAttributes = restaurantRepository
    //     .findRestaurantsByAttributesExact(searchString);

    Optional<List<RestaurantEntity>> allRestaurantsByItemAttributeOptional = restaurantRepository
        .findRestaurantsByAttributesExact(searchString);

    List<RestaurantEntity> allRestaurantsByItemAttributes = new ArrayList<>();

    if (allRestaurantsByItemAttributeOptional.isPresent()) {
      allRestaurantsByItemAttributes = allRestaurantsByItemAttributeOptional.get();
    }

    List<Restaurant> byItemAttributesRestaurants = allRestaurantsByItemAttributes.stream()
        .filter(restaurant -> {
          return isRestaurantCloseByAndOpen(restaurant,
            currentTime, latitude, longitude, servingRadiusInKms);
        }).map(restaurant -> {
          return modelMapperProvider.get().map(restaurant, Restaurant.class);
        }).collect(Collectors.toList());
    
    return byItemAttributesRestaurants;

  }





  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
    }

    return false;
  }



}

