package io.webrtc.signaling.loadgen;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Seeded target demand over actual v1 directory buckets, bounded by the declared user population. */
final class SkewTargetSelector {
    private final long seed,population,destinationPopulation,bucketPopulation;
    private final String destination;private final int hotBucket;
    private final long[] targets,cumulative;
    private final int[] offsets,counts;
    SkewTargetSelector(long seed,long users,String prefix,String[] directory,int destinationMultiplier,int bucketMultiplier) {
        if(users<2||users>10000000||prefix==null||prefix.isBlank()||prefix.length()>512||directory==null||directory.length!=16384
                ||!Set.of(1,5).contains(destinationMultiplier)||!Set.of(1,5).contains(bucketMultiplier))throw new IllegalArgumentException("Invalid bounded skew profile");
        var nativeDirectory=directory.clone();
        for(var cell:nativeDirectory)if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}"))throw new IllegalArgumentException("Incomplete native directory");
        this.seed=seed;population=users/2;counts=new int[16384];offsets=new int[16384];cumulative=new long[16384];
        var cells=new TreeMap<String,Long>();
        final MessageDigest hash;
        try{hash=MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
        for(long user=population;user<2*population;user++){int bucket=bucket(hash,prefix+user);counts[bucket]++;cells.merge(nativeDirectory[bucket],1L,Long::sum);}
        if(cells.size()>50)throw new IllegalArgumentException("Invalid bounded cell topology");
        var destinations=new ArrayList<>(cells.keySet());destination=destinationMultiplier==1?null:destinations.get((int)Math.floorMod(seed,destinations.size()));destinationPopulation=destination==null?0:cells.get(destination);
        var eligible=new ArrayList<Integer>();for(int bucket=0;bucket<counts.length;bucket++)if(counts[bucket]>0&&!nativeDirectory[bucket].equals(destination))eligible.add(bucket);
        if(bucketMultiplier!=1&&eligible.isEmpty())throw new IllegalArgumentException("SKEW_PROFILE_IMPOSSIBLE: no independent hot bucket");
        hotBucket=bucketMultiplier==1?-1:eligible.get((int)Math.floorMod(Long.rotateLeft(seed,23),eligible.size()));bucketPopulation=hotBucket<0?0:counts[hotBucket];
        long coldPopulation=population-destinationPopulation-bucketPopulation;
        long coldWeight=population-destinationMultiplier*destinationPopulation-bucketMultiplier*bucketPopulation;
        if(coldPopulation<=0||coldWeight<0)throw new IllegalArgumentException("SKEW_PROFILE_IMPOSSIBLE: requested hot shares exceed target population");
        int offset=0;long total=0;
        for(int bucket=0;bucket<counts.length;bucket++){
            offsets[bucket]=offset;offset+=counts[bucket];
            long weight=nativeDirectory[bucket].equals(destination)?destinationMultiplier*coldPopulation:bucket==hotBucket?bucketMultiplier*coldPopulation:coldWeight;
            total=Math.addExact(total,Math.multiplyExact(weight,counts[bucket]));cumulative[bucket]=total;
        }
        if(total!=Math.multiplyExact(population,coldPopulation))throw new IllegalStateException("Skew population accounting mismatch");
        targets=new long[(int)population];var next=offsets.clone();
        for(long user=population;user<2*population;user++)targets[next[bucket(hash,prefix+user)]++]=user;
    }
    long target(long ordinal) {
        if(ordinal<0)throw new IllegalArgumentException("Original arrival ordinal required");
        long draw=Long.remainderUnsigned(mix(seed^ordinal),cumulative[cumulative.length-1]);int low=0,high=cumulative.length;
        while(low<high){int middle=(low+high)>>>1;if(cumulative[middle]<=draw)low=middle+1;else high=middle;}
        int rank=(int)Long.remainderUnsigned(mix(seed^ordinal^0xd1b54a32d192ed03L),counts[low]);return targets[offsets[low]+rank];
    }
    private static long mix(long value){value=(value^(value>>>30))*0xbf58476d1ce4e5b9L;value=(value^(value>>>27))*0x94d049bb133111ebL;return value^(value>>>31);}
    private static int bucket(MessageDigest hash,String user){var bytes=hash.digest(user.getBytes(StandardCharsets.UTF_8));return ((bytes[6]&63)<<8)|(bytes[7]&255);}
    String destinationCell(){return destination;}int hotBucket(){return hotBucket;}
    long destinationUsers(){return destinationPopulation;}long bucketUsers(){return bucketPopulation;}long targetUsers(){return population;}
}
