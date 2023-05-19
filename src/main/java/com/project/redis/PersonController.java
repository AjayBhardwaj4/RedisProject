package com.project.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class PersonController {
    private final String KEY_PREFIX = "per::";
    private final String PERSON_LIST_KEY = "per_list";
    private final String PERSON_HASH_KEY_PREFIX = "per_hash::";

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    /**
     *   String ops
     *   Key: String
     *   Value: Object
     **/
    @PostMapping("/string/person")
    public void savePerson(@RequestBody Person person) {
        if(person.getId() == 0) {
            return;
        }
        String key = getKey(person.getId());
        redisTemplate.opsForValue().set(key, person);
    }

    @GetMapping("/string/person")
    public Person getPerson(@RequestParam("id") int id) {
        return (Person) redisTemplate.opsForValue().get(getKey(id));
    }

    private String getKey(long id) {
        return KEY_PREFIX + String.valueOf(id);
    }

    /**
     *   String ops
     *   Key: String
     *   Value: List<Object>
     **/
/*
    @PostMapping("/lpush/person")
    public void lpush(@RequestBody Person person) {
        redisTemplate.opsForList().leftPushAll(PERSON_LIST_KEY, person);
    }

    @PostMapping("/rpush/person")
    public void rpush(@RequestBody Person person) {
        redisTemplate.opsForList().rightPush(PERSON_LIST_KEY, person);
    }
*/
    @PostMapping("/lpush/person")
    public void lpush(@RequestBody List<Person> people) throws Exception {
        List<Object> objects = Arrays.asList(people.toArray());
        redisTemplate.opsForList().leftPushAll(PERSON_LIST_KEY, objects);
    }

    @PostMapping("/rpush/person")
    public void rpush(@RequestBody List<Person> people) throws Exception {
        List<Object> objects = Arrays.asList(people.toArray());
        redisTemplate.opsForList().rightPushAll(PERSON_LIST_KEY, objects);
    }
    @DeleteMapping("/lpop/person")
    public List<Person> lpop(@RequestParam(value = "count", required = false, defaultValue = "1") int count) {
        return redisTemplate.opsForList().leftPop(PERSON_LIST_KEY, count)
                .stream()
                .map(x -> (Person)x)
                .collect(Collectors.toList());
    }

    @DeleteMapping("/rpop/person")
    public List<Person> rpop(@RequestParam(value = "count", required = false, defaultValue = "1") int count) {
        return redisTemplate.opsForList().rightPop(PERSON_LIST_KEY, count)
                .stream()
                .map(x -> (Person) x)
                .collect(Collectors.toList());
    }

    @GetMapping("lrange/person")
    public List<Person> lrange(@RequestParam(value = "start", required = false, defaultValue = "0") int start,
                               @RequestParam(value = "end", required = false, defaultValue = "-1") int end) {

        return redisTemplate.opsForList().range(PERSON_LIST_KEY, start, end)
                .stream()
                .map(x -> (Person) x)
                .collect(Collectors.toList());
    }

    /**
     *   Hash ops
     *   Key: String
     *   Field: String
     *   Value: String
     **/
    @PostMapping("/hash/person")
    public void savePersonInHash(@RequestBody List<Person> people) {
        people.stream()
                .filter(person -> person.getId() != 0)
                .forEach(person -> {
                    Map map = objectMapper.convertValue(person, Map.class);
                    redisTemplate.opsForHash().putAll(getHashKey(person.getId()), map);
                    redisTemplate.expire(getHashKey(person.getId()), Duration.ofHours(24));
                });
    }

    @GetMapping("/hash/person/all")
    public List<Person> getPeople(@RequestParam("ids") List<Integer> peopleIds) {
        return peopleIds.stream()
                .map(i -> redisTemplate.opsForHash().entries((getHashKey(i))))
                .map(entryMap -> objectMapper.convertValue(entryMap, Person.class))
                .collect(Collectors.toList());
    }

    private String getHashKey(long id) {
        return PERSON_HASH_KEY_PREFIX + String.valueOf(id);
    }
}
