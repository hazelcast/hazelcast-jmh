package com.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

@State(value = Scope.Thread)
public class SerializableFactoryBenchmark {

    public static final int TYPE_COUNT = 5;
    private final Person[] persons = new Person[TYPE_COUNT];

    private ArrayDataSerializableFactory arrayFactory;
    private OrderedCaseFactory orderedCaseFactory;
    private DataSerializableFactory unorderedCaseFactory;

    @Setup
    public void setUp() {
        for (int k = 0; k < persons.length; k++) {
            persons[k] = new Person();
        }

        ConstructorFunction<Integer, IdentifiedDataSerializable>[] functions = new ConstructorFunction[TYPE_COUNT];
        for (int k = 0; k < TYPE_COUNT; k++) {
            final int index = k;
            functions[k] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                @Override
                public IdentifiedDataSerializable createNew(Integer arg) {
                    return persons[index];
                }
            };
        }
        arrayFactory = new ArrayDataSerializableFactory(functions);
        orderedCaseFactory = new OrderedCaseFactory();
        unorderedCaseFactory = new UnorderedCaseFactory();
    }

    @GenerateMicroBenchmark
    @OperationsPerInvocation(100000000)
    public void performance_DataSerializableFactory() {
        int typeId = 0;
        for (int k = 0; k < 100000000; k++) {
            IdentifiedDataSerializable x = arrayFactory.create(typeId);
            if (x == null) {
                System.out.println("To prevent optimizing dumb code");
            }
            typeId++;
            if (typeId >= TYPE_COUNT) {
                typeId = 0;
            }
        }
    }

    @GenerateMicroBenchmark
    @OperationsPerInvocation(100000000)
    public void performance_orderedCase() {
        int typeId = 0;
        for (int k = 0; k < 100000000; k++) {
            IdentifiedDataSerializable x = orderedCaseFactory.create(typeId);
            if (x == null) {
                System.out.println("To prevent optimizing dumb code");
            }
            typeId++;
            if (typeId >= TYPE_COUNT) {
                typeId = 0;
            }
        }
    }

//    @GenerateMicroBenchmark
//    @OperationsPerInvocation(100000000)
//    public void performance_unorderedCase() {
//        int typeId = 0;
//        for (int k = 0; k < 100000000; k++) {
//            IdentifiedDataSerializable x = unorderedCaseFactory.create(typeId);
//            if (x == null) {
//                System.out.println("To prevent optimizing dumb code");
//            }
//            typeId++;
//            if (typeId >= TYPE_COUNT) {
//                typeId = 0;
//            }
//        }
//    }

    public class OrderedCaseFactory implements DataSerializableFactory {
        public Person create(int typeId) {
            switch (typeId) {
                case 0:
                    return persons[0];
                case 1:
                    return persons[1];
                case 2:
                    return persons[2];
                case 3:
                    return persons[3];
                case 4:
                    return persons[4];
/*
                case 5:
                    return persons[5];
                case 6:
                    return persons[6];
                case 7:
                    return persons[7];
                case 8:
                    return persons[8];
                case 9:
                    return persons[9];
*/
               /* case 10:
                    return persons[10];
                case 11:
                    return persons[11];
                case 12:
                    return persons[12];
                case 13:
                    return persons[13];
                case 14:
                    return persons[14];
                case 15:
                    return persons[15];
                case 16:
                    return persons[16];
                case 17:
                    return persons[17];
                case 18:
                    return persons[18];
                case 19:
                    return persons[19];
                case 20:
                    return persons[20];
                case 21:
                    return persons[21];
                case 22:
                    return persons[22];
                case 23:
                    return persons[23];
                case 24:
                    return persons[24];
                case 25:
                    return persons[25];
                case 26:
                    return persons[26];
                case 27:
                    return persons[27];
                case 28:
                    return persons[28];
                case 29:
                    return persons[29];
                case 30:
                    return persons[30];
                case 31:
                    return persons[31];
                case 32:
                    return persons[32];
                case 33:
                    return persons[33];
                case 34:
                    return persons[34];
                case 35:
                    return persons[35];
                case 36:
                    return persons[36];
                case 37:
                    return persons[37];
                case 38:
                    return persons[38];
                case 39:
                    return persons[39];
                case 40:
                    return persons[40];
                case 41:
                    return persons[41];
                case 42:
                    return persons[42];
                case 43:
                    return persons[43];
                case 44:
                    return persons[44];
                case 45:
                    return persons[45];
                case 46:
                    return persons[46];
                case 47:
                    return persons[47];
                case 48:
                    return persons[48];
                case 49:
                    return persons[49];
                case 50:
                    return persons[50];
                case 51:
                    return persons[51];
                case 52:
                    return persons[52];
                case 53:
                    return persons[53];
                case 54:
                    return persons[54];
                case 55:
                    return persons[55];
                case 56:
                    return persons[56];
                case 57:
                    return persons[57];
                case 58:
                    return persons[58];
                case 59:
                    return persons[59];
                case 60:
                    return persons[50];
                case 61:
                    return persons[61];
                case 62:
                    return persons[62];
                case 63:
                    return persons[63];
                case 64:
                    return persons[64];
                case 65:
                    return persons[65];
                case 66:
                    return persons[66];
                case 67:
                    return persons[67];
                case 68:
                    return persons[68];
                case 69:
                    return persons[69];
                case 70:
                    return persons[70];
                case 71:
                    return persons[71];
                case 72:
                    return persons[72];
                case 73:
                    return persons[73];
                case 74:
                    return persons[74];
                case 75:
                    return persons[75];
                case 76:
                    return persons[76];
                case 77:
                    return persons[77];
                case 78:
                    return persons[78];
                case 79:
                    return persons[79];
                case 80:
                    return persons[80];
                case 81:
                    return persons[81];
                case 82:
                    return persons[82];
                case 83:
                    return persons[83];
                case 84:
                    return persons[84];
                case 85:
                    return persons[85];
                case 86:
                    return persons[86];
                case 87:
                    return persons[87];
                case 88:
                    return persons[88];
                case 89:
                    return persons[89];
                case 90:
                    return persons[90];
                case 91:
                    return persons[91];
                case 92:
                    return persons[92];
                case 93:
                    return persons[93];
                case 94:
                    return persons[94];
                case 95:
                    return persons[95];
                case 96:
                    return persons[96];
                case 97:
                    return persons[97];
                case 98:
                    return persons[98];
                case 99:
                    return persons[99];*/
                default:
                    return null;
            }
        }
    }

    public class UnorderedCaseFactory implements DataSerializableFactory {
        public Person create(int typeId) {
            switch (typeId) {
                case 6:
                    return persons[6];
                case 16:
                    return persons[16];
                case 51:
                    return persons[51];
                case 52:
                    return persons[52];
                case 53:
                    return persons[53];
                case 54:
                    return persons[54];
                    case 4:
                    return persons[4];
                case 24:
                    return persons[24];
                case 25:
                    return persons[25];
                case 26:
                    return persons[26];
                case 32:
                    return persons[32];
                case 33:
                    return persons[33];
                case 15:
                    return persons[15];
                case 19:
                    return persons[19];
                case 14:
                    return persons[14];
                case 20:
                    return persons[20];
                case 11:
                    return persons[11];
                case 12:
                    return persons[12];
                case 31:
                    return persons[31];
                case 9:
                    return persons[9];
                case 10:
                    return persons[10];
                case 22:
                    return persons[22];
                case 23:
                    return persons[23];
                case 39:
                    return persons[39];
                case 27:
                    return persons[27];
                case 34:
                    return persons[34];
                case 35:
                    return persons[35];
                case 5:
                    return persons[5];
                case 0:
                    return persons[0];
                case 13:
                    return persons[13];
                case 2:
                    return persons[2];
                case 55:
                    return persons[55];
                case 56:
                    return persons[56];
                case 74:
                    return persons[74];
                case 75:
                    return persons[75];
                case 76:
                    return persons[76];
                case 17:
                    return persons[17];
                case 38:
                    return persons[38];
                case 18:
                    return persons[18];
                case 42:
                    return persons[42];
                case 43:
                    return persons[43];
                case 44:
                    return persons[44];
                case 45:
                    return persons[45];
                        case 28:
                    return persons[28];
                case 29:
                    return persons[29];
                case 30:
                    return persons[30];
                case 40:
                    return persons[40];
                case 41:
                    return persons[41];
                      case 92:
                    return persons[92];
                case 93:
                    return persons[93];
                case 94:
                    return persons[94];
                case 95:
                    return persons[95];
                case 96:
                    return persons[96];
                case 97:
                    return persons[97];
                case 77:
                    return persons[77];
                case 78:
                    return persons[78];
                case 79:
                    return persons[79];
                case 80:
                    return persons[80];
                case 81:
                    return persons[81];
                case 82:
                    return persons[82];
                case 83:
                    return persons[83];
                case 57:
                    return persons[57];
                case 58:
                    return persons[58];
                case 59:
                    return persons[59];
                case 60:
                    return persons[50];
                case 61:
                    return persons[61];
                case 89:
                    return persons[89];
                case 90:
                    return persons[90];
                case 91:
                    return persons[91];
                case 98:
                    return persons[98];
                case 99:
                    return persons[99];
                case 62:
                    return persons[62];
                case 46:
                    return persons[46];
                case 47:
                    return persons[47];
                case 48:
                    return persons[48];
                case 49:
                    return persons[49];
                case 50:
                    return persons[50];
                case 7:
                    return persons[7];
                case 8:
                    return persons[8];
                case 1:
                    return persons[1];
                case 36:
                    return persons[36];
                case 37:
                    return persons[37];
                case 3:
                    return persons[3];
                case 21:
                    return persons[21];
                case 63:
                    return persons[63];
                case 64:
                    return persons[64];
                case 65:
                    return persons[65];
                case 66:
                    return persons[66];
                case 67:
                    return persons[67];
                case 68:
                    return persons[68];
                case 69:
                    return persons[69];
                case 70:
                    return persons[70];
                case 71:
                    return persons[71];
                case 72:
                    return persons[72];
                case 73:
                    return persons[73];
                case 84:
                    return persons[84];
                case 85:
                    return persons[85];
                case 86:
                    return persons[86];
                case 87:
                    return persons[87];
                case 88:
                    return persons[88];

                default:
                    return null;
            }
        }
    }


    public class Person implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
            return 1;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
        }

        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
